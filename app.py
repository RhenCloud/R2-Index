import hashlib
import os
from datetime import datetime
from io import BytesIO
from pathlib import Path
from typing import Any, Dict, List

import boto3
from botocore.config import Config
from dotenv import load_dotenv
from flask import Flask, Response, abort, render_template, request, send_file
from PIL import Image

# 加载环境变量
load_dotenv()

app = Flask(__name__, static_url_path="/static", static_folder="static")


# 缩略图默认 TTL（秒），可通过环境变量覆盖
THUMB_TTL = int(os.getenv("THUMB_TTL_SECONDS", "3600"))


# 注册一个安全的 filesizeformat 过滤器，处理 None 和非数字值
@app.template_filter("filesizeformat")
def filesizeformat_filter(value):
    try:
        if value is None:
            return "-"
        num = float(value)  # 使用 float 而不是 int 以保持精度
    except Exception:
        return "-"

    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if num < 1024:
            # 对于字节，显示整数
            if unit == "B":
                return f"{int(num)}{unit}"
            # 其他单位保留两位小数
            return f"{num:.2f}{unit}"
        num = num / 1024.0
    return f"{num:.2f}PB"


# 注册一个文件图标过滤器
@app.template_filter("fileicon")
def fileicon_filter(filename):
    if not filename:
        return "fas fa-file"

    ext = filename.lower().split(".")[-1] if "." in filename else ""

    # 图片文件
    if ext in ["jpg", "jpeg", "png", "gif", "bmp", "webp", "svg"]:
        return "fas fa-image"

    # 音频文件
    if ext in ["mp3", "wav", "ogg", "flac", "m4a", "aac"]:
        return "fas fa-music"

    # 视频文件
    if ext in ["mp4", "webm", "avi", "mov", "wmv", "flv", "mkv"]:
        return "fas fa-video"

    # 文档文件
    if ext in ["pdf", "doc", "docx", "txt", "md", "rtf"]:
        return "fas fa-file-alt"

    # 压缩文件
    if ext in ["zip", "rar", "7z", "tar", "gz"]:
        return "fas fa-file-archive"

    # 代码文件
    if ext in ["py", "js", "html", "css", "java", "cpp", "c", "php"]:
        return "fas fa-file-code"

    # 表格文件
    if ext in ["xls", "xlsx", "csv"]:
        return "fas fa-file-excel"

    # 演示文件
    if ext in ["ppt", "pptx"]:
        return "fas fa-file-powerpoint"

    # 默认文件图标
    return "fas fa-file"


def get_s3_client():
    """
    创建并返回配置好的 S3 客户端，用于访问 R2 存储
    """
    endpoint = os.getenv("R2_ENDPOINT_URL")
    if not endpoint:
        # 更明确的错误，便于调试环境变量问题
        raise RuntimeError("R2_ENDPOINT_URL environment variable is not set")

    # 支持常见 AWS 环境变量名：AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY
    access_key = os.getenv("ACCESS_KEY_ID") or os.getenv("ACCESS_KEY_ID")
    secret_key = os.getenv("SECRET_ACCESS_KEY") or os.getenv("SECRET_ACCESS_KEY")

    if not access_key or not secret_key:
        raise RuntimeError("ACCESS_KEY_ID and SECRET_ACCESS_KEY must be set")

    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        config=Config(signature_version="s3v4"),
        region_name=os.getenv("R2_REGION", "auto"),
    )


def get_public_url(key: str) -> str:
    """
    生成对象的公共访问 URL
    """
    base_url = os.getenv("R2_PUBLIC_URL")
    if not base_url:
        return None
    return f"{base_url.rstrip('/')}/{key}"


def format_timestamp(timestamp) -> str:
    """
    格式化时间戳为人类可读的格式
    """
    if isinstance(timestamp, datetime):
        return timestamp.strftime("%Y-%m-%d %H:%M:%S")
    return str(timestamp)


def generate_presigned_url(
    s3_client, bucket_name: str, key: str, expires: int = None
) -> str:
    """为指定对象生成 presigned URL（GET）。"""
    if expires is None:
        try:
            expires = int(os.getenv("R2_PRESIGN_EXPIRES", "3600"))
        except Exception:
            expires = 3600

    try:
        url = s3_client.generate_presigned_url(
            "get_object", Params={"Bucket": bucket_name, "Key": key}, ExpiresIn=expires
        )
        return url
    except Exception:
        app.logger.exception("Failed to generate presigned URL for %s", key)
        return None


@app.route("/")
def index():
    """
    返回 R2 存储桶中的文件和目录列表的 HTML 页面。
    """
    try:
        s3_client = get_s3_client()
        bucket_name = os.getenv("R2_BUCKET_NAME")

        # 支持 prefix 查询参数（用于浏览子目录）
        prefix = request.args.get("prefix", "") or ""
        if prefix and not prefix.endswith("/"):
            prefix = prefix + "/"

        list_kwargs = {"Bucket": bucket_name, "Delimiter": "/"}
        if prefix:
            list_kwargs["Prefix"] = prefix

        # 列出指定前缀（根或子目录）下的对象
        response = s3_client.list_objects_v2(**list_kwargs)

        entries: List[Dict[str, Any]] = []
        if "Contents" in response:
            for obj in response["Contents"]:
                # 跳过等于 prefix 的条目（有时存在）
                key = obj.get("Key", "")
                if prefix and key == prefix:
                    continue
                if key.endswith("/"):
                    continue

                # 显示相对名称（去掉当前 prefix）
                rel_name = key[len(prefix) :] if prefix else key

                entry = {
                    "name": rel_name,
                    "key": key,
                    "size": obj.get("Size"),
                    "last_modified": format_timestamp(obj.get("LastModified")),
                    "is_dir": False,
                }

                # 添加公共访问 URL（如果配置了）
                public_url = get_public_url(key)
                if public_url:
                    entry["public_url"] = public_url

                # 添加 presigned URL（优先于后端代理预览）
                presigned = generate_presigned_url(s3_client, bucket_name, key)
                if presigned:
                    entry["presigned_url"] = presigned

                # 通过服务器访问的文件 URL（用于预览和缩略图）
                entry["file_url"] = get_file_url(key)
                entries.append(entry)

        # 添加当前前缀下的文件夹（CommonPrefixes）
        if "CommonPrefixes" in response:
            for p in response["CommonPrefixes"]:
                pref = p.get("Prefix")
                # 相对文件夹名
                rel = pref[len(prefix) :].rstrip("/") if prefix else pref.rstrip("/")
                entries.append({"name": rel, "key": pref, "is_dir": True})

        # 按照类型（目录在前）和名称排序
        entries.sort(key=lambda x: (not x.get("is_dir", False), x["name"]))

        # 构造面包屑导航
        crumbs = []
        if prefix:
            segs = prefix.rstrip("/").split("/")
            acc = ""
            for seg in segs:
                acc = acc + seg + "/"
                crumbs.append({"name": seg, "prefix": acc})

        return render_template(
            "index.html",
            entries=entries,
            current_prefix=prefix,
            crumbs=crumbs,
            current_year=datetime.now().year,
        )
    except Exception as e:
        app.logger.exception("Error listing R2 bucket")
        return render_template(
            "index.html", error=str(e), current_year=datetime.now().year
        )


@app.route("/<path:prefix_path>")
def browse(prefix_path):
    """漂亮的目录路由。将 URL /a/b 映射为 prefix 'a/b/' 并重用 index 的逻辑。"""
    # delegate to index-like logic but with provided prefix
    try:
        s3_client = get_s3_client()
        bucket_name = os.getenv("R2_BUCKET_NAME")

        prefix = prefix_path or ""
        if prefix and not prefix.endswith("/"):
            prefix = prefix + "/"

        list_kwargs = {"Bucket": bucket_name, "Delimiter": "/", "Prefix": prefix}
        response = s3_client.list_objects_v2(**list_kwargs)

        entries: List[Dict[str, Any]] = []
        if "Contents" in response:
            for obj in response["Contents"]:
                key = obj.get("Key", "")
                if prefix and key == prefix:
                    continue
                if key.endswith("/"):
                    continue
                rel_name = key[len(prefix) :] if prefix else key
                entry = {
                    "name": rel_name,
                    "key": key,
                    "size": obj.get("Size"),
                    "last_modified": format_timestamp(obj.get("LastModified")),
                    "is_dir": False,
                    "file_url": get_file_url(key),
                }
                entries.append(entry)

        if "CommonPrefixes" in response:
            for p in response["CommonPrefixes"]:
                pref = p.get("Prefix")
                rel = pref[len(prefix) :].rstrip("/") if prefix else pref.rstrip("/")
                entries.append({"name": rel, "key": pref, "is_dir": True})

        entries.sort(key=lambda x: (not x.get("is_dir", False), x["name"]))

        crumbs = []
        if prefix:
            segs = prefix.rstrip("/").split("/")
            acc = ""
            for seg in segs:
                acc = acc + seg + "/"
                crumbs.append({"name": seg, "prefix": acc})

        return render_template(
            "index.html",
            entries=entries,
            current_prefix=prefix,
            crumbs=crumbs,
            current_year=datetime.now().year,
        )
    except Exception as e:
        app.logger.exception("Error browsing R2 bucket")
        return render_template(
            "index.html", error=str(e), current_year=datetime.now().year
        )


@app.route("/file/<path:file_path>")
def serve_file(file_path):
    """通过服务器提供文件访问"""
    try:
        s3_client = get_s3_client()
        bucket_name = os.getenv("R2_BUCKET_NAME")

        # 获取文件的基本信息
        try:
            response = s3_client.head_object(Bucket=bucket_name, Key=file_path)
        except s3_client.exceptions.ClientError as e:
            if e.response["Error"]["Code"] == "404":
                abort(404)
            else:
                abort(500)

        # 获取文件对象
        file_obj = s3_client.get_object(Bucket=bucket_name, Key=file_path)

        # 设置响应头
        headers = {
            "Content-Type": file_obj["ContentType"],
            "Content-Length": str(file_obj["ContentLength"]),
        }

        # 使用 Response 流式传输文件内容
        return Response(
            file_obj["Body"].iter_chunks(), headers=headers, direct_passthrough=True
        )

    except Exception as e:
        app.logger.exception("Error serving file")
        abort(500)


def get_file_url(key: str) -> str:
    """生成通过服务器访问文件的 URL"""
    return f"/file/{key}"


@app.route("/thumb/<path:file_path>")
def thumb(file_path):
    """返回图片的缩略图，使用 Vercel Cache Headers 避免重复从 R2 拉取"""
    bucket_name = os.getenv("R2_BUCKET_NAME")

    # 设置更长的缓存控制头以支持浏览器本地缓存
    cache_headers = {
        "Cache-Control": f"public, max-age={THUMB_TTL}",
        "ETag": f'W/"{hashlib.md5(file_path.encode("utf-8")).hexdigest()}"',
    }

    # 先检查客户端是否已经有缓存版本
    etag = request.headers.get("If-None-Match")
    if etag and etag == cache_headers["ETag"]:
        return Response(status=304, headers=cache_headers)

    # 从 R2 获取原始对象并生成缩略图
    try:
        s3 = get_s3_client()
        try:
            obj = s3.get_object(Bucket=bucket_name, Key=file_path)
            data = obj["Body"].read()
        except Exception:
            app.logger.exception("Failed to fetch object for thumb: %s", file_path)
            response = send_file(
                os.path.join(app.static_folder, "thumb_placeholder.svg"),
                mimetype="image/svg+xml",
            )
            response.headers.update(cache_headers)
            return response

        try:
            img = Image.open(BytesIO(data))
            img = img.convert("RGB")
            img.thumbnail((320, 320))
            buf = BytesIO()
            img.save(buf, "JPEG", quality=80, optimize=True)
            buf.seek(0)
            thumb_bytes = buf.getvalue()

            response = Response(thumb_bytes, mimetype="image/jpeg")
            response.headers.update(cache_headers)
            return response
        except Exception:
            app.logger.exception("Failed to generate thumbnail for %s", file_path)
            response = send_file(
                os.path.join(app.static_folder, "thumb_placeholder.svg"),
                mimetype="image/svg+xml",
            )
            response.headers.update(cache_headers)
            return response
    except Exception:
        app.logger.exception("Unexpected error in thumb endpoint")
        response = send_file(
            os.path.join(app.static_folder, "thumb_placeholder.svg"),
            mimetype="image/svg+xml",
        )
        response.headers.update(cache_headers)
        return response


# 添加路由以提供Service Worker文件
@app.route("/static/sw.js")
def sw():
    return send_file(
        os.path.join(app.static_folder, "sw.js"), mimetype="application/javascript"
    )


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    host = os.environ.get("HOST", "0.0.0.0")
    app.run(host=host, port=port, debug=True)
