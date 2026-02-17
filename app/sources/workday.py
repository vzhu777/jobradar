import hashlib


def _bullet_fields_to_dict(bf):
    """
    Workday sometimes returns bulletFields as:
      - dict
      - list of {"label": "...", "value": "..."}
    Normalize to a dict.
    """
    if isinstance(bf, dict):
        return bf

    if isinstance(bf, list):
        out = {}
        for item in bf:
            if isinstance(item, dict):
                label = item.get("label") or item.get("name")
                value = item.get("value")
                if label:
                    out[label] = value
        return out

    return {}


def normalize_workday(company_name: str, board_url: str, job: dict) -> dict:
    title = job.get("title") or job.get("jobTitle") or "Unknown title"
    location = job.get("locationsText") or job.get("location") or ""

    external_path = job.get("externalPath") or job.get("path") or ""
    url = job.get("externalUrl")

    if not url:
        url = board_url.rstrip("/") + external_path

    # Handle bulletFields safely
    bf = _bullet_fields_to_dict(job.get("bulletFields"))

    job_req = (
        bf.get("Req ID")
        or bf.get("Requisition ID")
        or bf.get("Job Requisition ID")
        or bf.get("jobReqId")
        or job.get("jobReqId")
        or job.get("id")
    )

    source_job_id = str(job_req or url)

    content_hash = hashlib.sha256(
        f"{company_name}|{title}|{location}|{url}".encode()
    ).hexdigest()

    return {
        "company": company_name,
        "title": title,
        "location": location,
        "url": url,
        "description": job.get("description", "") or "",
        "source": "workday",
        "source_job_id": source_job_id,
        "posted_at": None,
        "content_hash": content_hash,
        "is_active": True,
    }
