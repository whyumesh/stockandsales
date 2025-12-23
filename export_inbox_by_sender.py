# -*- coding: utf-8 -*-
"""
Export shared mailbox Inbox messages by sender (FIXED - guarantees all emails downloaded).

Key fixes:
- ENABLE_GUARANTEE_PASS_FOR_ALL_SENDERS = True (ensures every sender is checked)
- DATE_RANGE_DAYS = None (scans all time by default)
- Improved sender matching (checks both normalized and original addresses)
- Multiple sender extraction methods for reliability
"""

import win32com.client as win32
import os
import sys
import traceback
from datetime import datetime, timedelta
import pandas as pd
import re
import hashlib
from collections import Counter

# ============================================================
# CONFIG — SHARED MAILBOX ONLY
# ============================================================
SHARED_MAILBOX_ADDRESS = "stockandsales@abbott.com"  # <-- verify exact address
EMAIL_LIST_XLSX = r"C:\Users\PAWARUX1\Desktop\emailautomate_stockist\sender_list.xlsx"
SAVE_DIR = r"C:\Users\PAWARUX1\Desktop\emailautomate_stockist\corrected emails"
LOG_XLSX = None  # None => create new log per run (ExportLog_YYYYMMDD_HHMMSS.xlsx)

# Scan options (shared mailbox Inbox + optional subfolders)
INCLUDE_SUBFOLDERS = True      # Include subfolders to catch more emails
ONLY_UNREAD = False
DATE_RANGE_DAYS = None         # Scan ALL time to ensure nothing is missed

# Guarantee pass: ENABLED - ensures every sender in list is checked thoroughly
ENABLE_GUARANTEE_PASS_FOR_ALL_SENDERS = True

# File safety
MAX_SUBJECT_LEN = 90
MAX_PATH_LEN = 235
USE_UNICODE_MSG = True  # try olMSGUnicode (9) then olMSG (3)

# Prevent repeated processing of same item *within one run*
PREVENT_REPEAT_PROCESSING_IN_RUN = True

# Keep False — do NOT remove duplicates by InternetMessageId
AVOID_DUPLICATES_BY_INTERNET_MESSAGE_ID = False

# Progress logging cadence
PROGRESS_EVERY = 500  # print a progress line every N scanned items

# ============================================================
# HELPERS
# ============================================================
ANY_EMAIL_RE = re.compile(r"[A-Z0-9._%+\-]+@[A-Z0-9.\-]+\.[A-Z]{2,}", re.IGNORECASE)
INVISIBLE = ["\u200b", "\u200c", "\u200d", "\ufeff", "\u00a0"]

def debug(msg: str):
    print(msg, flush=True)

def ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)

def normalize_addr(s: str) -> str:
    if s is None:
        return ""
    s = str(s)
    for ch in INVISIBLE:
        s = s.replace(ch, "")
    s = s.strip().replace("mailto:", "").strip().lower()  # Changed to .lower() for consistency
    return s

def plus_alias_base(addr: str) -> str:
    """abc+tag@domain.com -> abc@domain.com"""
    addr = normalize_addr(addr)
    if "+" in addr and "@" in addr:
        local, _, domain = addr.partition("@")
        local_base = local.split("+")[0]
        return f"{local_base}@{domain}"
    return addr

def excel_safe_dt_str(dt):
    if dt is None:
        return ""
    try:
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return str(dt)

def safe_filename(name: str) -> str:
    invalid = '<>:"/\\\n?*'
    cleaned = "".join(c for c in (name or "") if c not in invalid)
    cleaned = cleaned.strip().rstrip(".")
    cleaned = cleaned[:MAX_SUBJECT_LEN] if len(cleaned) > MAX_SUBJECT_LEN else cleaned
    return cleaned if cleaned else "No Subject"

def split_emails(cell_value: str):
    """Split comma/semicolon/whitespace separated emails in Excel cell and validate."""
    if cell_value is None:
        return []
    s = str(cell_value).replace(";", ",").replace("\n", ",").replace("\t", ",")
    parts = [p.strip() for p in s.split(",")]
    out = []
    for p in parts:
        p2 = normalize_addr(p)
        if p2 and ANY_EMAIL_RE.fullmatch(p2):
            out.append(p2)
    return out

def read_sender_whitelist_from_excel(path: str):
    """
    Reads Excel and collects emails from any column whose header contains 'email' (case-insensitive).
    Stores BOTH normalized and original forms for matching.
    """
    if not os.path.isfile(path):
        raise FileNotFoundError(f"Excel file not found: {path}")
    df = pd.read_excel(path, engine="openpyxl")
    email_cols = [c for c in df.columns if "email" in str(c).lower()]
    if not email_cols:
        raise ValueError("Expected at least one column containing 'email' in the Excel file.")
    
    whitelist_map = {}
    whitelist_rows = []
    whitelist_norm_set = set()
    whitelist_all_variants = set()  # Store all possible variants
    
    for _, row in df.iterrows():
        for col in email_cols:
            raw = row.get(col, None)
            raw_str = "" if raw is None else str(raw)
            norms = split_emails(raw_str)
            if not norms:
                whitelist_rows.append({"original": raw_str, "normalized": "", "valid_email": False, "column": col})
                continue
            for norm in norms:
                base = plus_alias_base(norm)  # normalize plus-addressing
                whitelist_norm_set.add(base)
                whitelist_all_variants.add(norm)  # Keep original normalized form too
                whitelist_all_variants.add(base)  # And the base form
                if base not in whitelist_map:
                    whitelist_map[base] = raw_str.strip()
                whitelist_rows.append({"original": raw_str, "normalized": base, "valid_email": True, "column": col})
    
    if not whitelist_norm_set:
        raise ValueError("No valid email IDs found in the Excel file.")
    return whitelist_norm_set, whitelist_map, whitelist_rows, whitelist_all_variants

# ============================================================
# Outlook iteration / filters
# ============================================================
def iter_items(items):
    """Reliable COM iteration (prevents skipping)."""
    try:
        it = items.GetFirst()
        while it:
            yield it
            it = items.GetNext()
    except Exception:
        for it in items:
            yield it

def restrict_items(items, only_unread=False, date_range_days=None):
    """Apply standard filters first; sender-specific Restrict applied later per pass."""
    filters = []
    if only_unread:
        filters.append("[Unread] = True")
    if date_range_days and isinstance(date_range_days, int) and date_range_days > 0:
        cutoff = (datetime.now() - timedelta(days=date_range_days)).strftime("%m/%d/%Y %I:%M %p")
        filters.append(f"[ReceivedTime] >= '{cutoff}'")
    if filters:
        return items.Restrict(" AND ".join(filters))
    return items

def get_item_unique_key(item):
    try:
        entry_id = getattr(item, "EntryID", "") or ""
    except Exception:
        entry_id = ""
    try:
        store_id = item.Parent.StoreID
    except Exception:
        store_id = ""
    return f"{store_id}\n{entry_id}"

# ============================================================
# Headers/MAPI extraction
# ============================================================
def get_headers_text(item) -> str:
    """Try Unicode headers (0x007D001F) then ANSI (0x007D001E)."""
    try:
        pa = item.PropertyAccessor
        for prop in (
            "http://schemas.microsoft.com/mapi/proptag/0x007D001F",
            "http://schemas.microsoft.com/mapi/proptag/0x007D001E",
        ):
            try:
                val = pa.GetProperty(prop)
                if isinstance(val, str) and val.strip():
                    return val
            except Exception:
                pass
    except Exception:
        pass
    return ""

def extract_emails_from_text(text: str):
    out = set()
    for m in ANY_EMAIL_RE.findall(text or ""):
        normalized = normalize_addr(m)
        out.add(normalized)
        out.add(plus_alias_base(normalized))  # Add both forms
    return out

def collect_sender_addresses(item) -> set:
    """
    Collect ALL possible sender SMTP addresses using multiple methods:
    - SenderEmailAddress
    - SenderName
    - Sender.GetExchangeUser().PrimarySmtpAddress
    - Internet headers (From, Reply-To, Return-Path, Sender)
    """
    candidates = set()
    
    # Method 1: Direct SenderEmailAddress
    try:
        addr = normalize_addr(getattr(item, "SenderEmailAddress", "") or "")
        if addr and "@" in addr and not addr.startswith("/o="):
            candidates.add(addr)
            candidates.add(plus_alias_base(addr))
    except Exception:
        pass

    # Method 2: SenderName (sometimes contains email)
    try:
        sender_name = normalize_addr(getattr(item, "SenderName", "") or "")
        if sender_name and "@" in sender_name:
            candidates.add(sender_name)
            candidates.add(plus_alias_base(sender_name))
    except Exception:
        pass

    # Method 3: Exchange user primary SMTP
    try:
        sender = getattr(item, "Sender", None)
        if sender:
            exuser = sender.GetExchangeUser()
            if exuser:
                smtp = normalize_addr(getattr(exuser, "PrimarySmtpAddress", "") or "")
                if smtp:
                    candidates.add(smtp)
                    candidates.add(plus_alias_base(smtp))
    except Exception:
        pass

    # Method 4: Parse Internet headers thoroughly
    try:
        headers = get_headers_text(item)
        if headers:
            header_emails = extract_emails_from_text(headers)
            candidates.update(header_emails)
    except Exception:
        pass
    
    return candidates

# ============================================================
# Saving .msg — unique filenames (no skip)
# ============================================================
def received_parts(item):
    """Return (yyyyMMdd, HHmmss) from ReceivedTime; fallback to now."""
    dt = getattr(item, "ReceivedTime", None)
    if not dt:
        dt = datetime.now()
    try:
        return dt.strftime("%Y%m%d"), dt.strftime("%H%M%S")
    except Exception:
        now = datetime.now()
        return now.strftime("%Y%m%d"), now.strftime("%H%M%S")

def make_msg_token(item, path_hint: str = "") -> str:
    """
    Produce a short token per email to guarantee unique filenames.
    Prefer InternetMessageId; fallback to EntryID + store + subject hash.
    """
    base = ""
    try:
        pa = item.PropertyAccessor
        msgid = pa.GetProperty("http://schemas.microsoft.com/mapi/proptag/0x1035001F")
        if isinstance(msgid, str) and msgid.strip():
            base = msgid.strip()
    except Exception:
        base = ""
    if not base:
        try:
            entry_id = getattr(item, "EntryID", "") or ""
            store_id = item.Parent.StoreID if item.Parent else ""
            subj = getattr(item, "Subject", "") or ""
            base = f"{store_id}\n{entry_id}\n{subj}\n{path_hint}"
        except Exception:
            base = f"{path_hint}\nfallback"
    return hashlib.sha1(base.encode("utf-8", errors="ignore")).hexdigest()[:8]

def unique_safe_path(base_dir: str, ymd: str, hms: str, subject: str, token: str) -> str:
    """
    <yyyyMMdd-HHmmss>_<token> - <subject>.msg
    Ensures uniqueness and Windows-safe path length.
    """
    fname = f"{ymd}-{hms}_{token} - {subject}.msg"
    path = os.path.join(base_dir, fname)
    if len(path) > MAX_PATH_LEN:
        subject2 = subject[:45]
        fname = f"{ymd}-{hms}_{token} - {subject2}.msg"
        path = os.path.join(base_dir, fname)

    # If file exists (rerun), append incremental suffix (we do not skip)
    if os.path.exists(path):
        i = 1
        while True:
            alt = os.path.join(base_dir, f"{ymd}-{hms}_{token}_{i}.msg")
            if len(alt) <= MAX_PATH_LEN and not os.path.exists(alt):
                return alt
            i += 1
    return path

def save_mail_item(item, base_dir: str, processed_msgids: set, uniq_key: str):
    ensure_dir(base_dir)

    if AVOID_DUPLICATES_BY_INTERNET_MESSAGE_ID:
        try:
            pa = item.PropertyAccessor
            msgid = pa.GetProperty("http://schemas.microsoft.com/mapi/proptag/0x1035001F")
            if isinstance(msgid, str):
                msgid = msgid.strip()
            if msgid:
                if msgid in processed_msgids:
                    return (False, None, "Duplicate (InternetMessageId)")
                processed_msgids.add(msgid)
        except Exception:
            pass

    ymd, hms = received_parts(item)
    subject = safe_filename(getattr(item, "Subject", "No Subject"))
    token = make_msg_token(item, base_dir)
    path = unique_safe_path(base_dir, ymd, hms, subject, token)

    formats = [9, 3] if USE_UNICODE_MSG else [3, 9]
    last_err = ""
    for fmt in formats:
        try:
            item.SaveAs(path, fmt)
            return (True, path, "")
        except Exception as e:
            last_err = str(e)
    # try copy-save once
    try:
        cp = item.Copy()
        for fmt in formats:
            try:
                cp.SaveAs(path, fmt)
                return (True, path, "")
            except Exception as e:
                last_err = str(e)
    except Exception as e:
        last_err = str(e)
    return (False, None, last_err or "Unknown SaveAs failure")

# ============================================================
# Processing (shared Inbox + subfolders)
# ============================================================
def process_folder(
    folder,
    whitelist_norm_set,
    whitelist_all_variants,
    whitelist_map,
    save_dir,
    only_unread,
    date_range_days,
    processed_keys,
    processed_msgids,
    stats,
    details_rows,
    error_rows,
    seen_counter,
    totals,
):
    items = folder.Items
    items.Sort("[ReceivedTime]", True)
    items = restrict_items(items, only_unread=only_unread, date_range_days=date_range_days)

    # (Optional) visibility of workload size
    try:
        debug(f"Folder '{folder.FolderPath}' filtered count ~ {items.Count}")
    except Exception:
        pass

    for item in iter_items(items):
        totals["scanned"] += 1
        if PROGRESS_EVERY and (totals["scanned"] % PROGRESS_EVERY == 0):
            debug(f"Scanned {totals['scanned']:,} ... Saved={totals['saved']:,}, RepeatSkipped={totals['repeat_skipped']:,}")

        if getattr(item, "Class", None) != 43:  # MailItem
            continue

        uniq_key = get_item_unique_key(item)
        if PREVENT_REPEAT_PROCESSING_IN_RUN and uniq_key:
            if uniq_key in processed_keys:
                totals["repeat_skipped"] += 1
                continue
            processed_keys.add(uniq_key)

        cands = collect_sender_addresses(item)
        for addr in cands:
            seen_counter[addr] += 1

        # Match against ALL variants (normalized, base, original)
        matched_norm = ""
        for c in cands:
            if c in whitelist_all_variants:
                # Find the base form in whitelist_norm_set
                base = plus_alias_base(c)
                if base in whitelist_norm_set:
                    matched_norm = base
                    break
                # If exact match exists, use it
                if c in whitelist_norm_set:
                    matched_norm = c
                    break

        if not matched_norm:
            continue  # not a sender we track

        bucket = stats.setdefault(matched_norm, {"matched": 0, "saved": 0, "first_received": "", "last_received": ""})
        bucket["matched"] += 1

        saved, path, err = save_mail_item(item, save_dir, processed_msgids, uniq_key)
        if saved:
            bucket["saved"] += 1
            totals["saved"] += 1
            details_rows.append(
                {
                    "emailID_matched_norm": matched_norm,
                    "emailID_matched_display": whitelist_map.get(matched_norm, matched_norm),
                    "SenderCandidates": ", ".join(sorted(cands)),
                    "Subject": getattr(item, "Subject", ""),
                    "ReceivedTime": excel_safe_dt_str(getattr(item, "ReceivedTime", None)),
                    "FolderPath": folder.FolderPath,
                    "SavedPath": path,
                }
            )
        else:
            totals["save_failed"] += 1
            error_rows.append(
                {
                    "emailID_matched_norm": matched_norm,
                    "SenderCandidates": ", ".join(sorted(cands)),
                    "Subject": getattr(item, "Subject", ""),
                    "ReceivedTime": excel_safe_dt_str(getattr(item, "ReceivedTime", None)),
                    "FolderPath": folder.FolderPath,
                    "Error": err,
                }
            )

def walk_subfolders(folder, **kwargs):
    for sub in folder.Folders:
        kwargs["totals"]["folders_processed"] += 1
        process_folder(sub, **kwargs)
        walk_subfolders(sub, **kwargs)

# ============================================================
# Guarantee pass - Enhanced to try multiple Restrict approaches
# Guarantee pass - Enhanced to try multiple Restrict approaches
# ============================================================
def restrict_by_sender_multiple_methods(folder, sender_email: str, only_unread: bool, date_range_days):
    """Try multiple Restrict approaches to catch all emails from sender."""
    items = folder.Items
    items.Sort("[ReceivedTime]", True)
    items = restrict_items(items, only_unread=only_unread, date_range_days=date_range_days)
    
    s = sender_email.replace("'", "''")
    
    # Try multiple query formats
    queries = [
        f"[SenderEmailAddress] = '{s}'",
        f"[SenderName] = '{s}'",
    ]
    
    all_found = []
    for query in queries:
        try:
            restricted = items.Restrict(query)
            for item in iter_items(restricted):
                all_found.append(item)
        except Exception:
            pass
    
    return all_found

def guarantee_pass_for_sender_in_folder(
    folder,
    sender,
    whitelist_map,
    save_dir,
    only_unread,
    date_range_days,
    processed_keys,
    processed_msgids,
    stats,
    details_rows,
    error_rows,
    totals,
):
    bucket = stats.setdefault(sender, {"matched": 0, "saved": 0, "first_received": "", "last_received": ""})

    items_found = restrict_by_sender_multiple_methods(folder, sender, only_unread, date_range_days)
    
    for item in items_found:
        if getattr(item, "Class", None) != 43:
            continue

        uniq_key = get_item_unique_key(item)
        if PREVENT_REPEAT_PROCESSING_IN_RUN and uniq_key:
            if uniq_key in processed_keys:
                continue
            processed_keys.add(uniq_key)

        saved, path, err = save_mail_item(item, save_dir, processed_msgids, uniq_key)
        bucket["matched"] += 1
        if saved:
            bucket["saved"] += 1
            totals["saved"] += 1
            totals["second_pass_saved"] += 1
            details_rows.append(
                {
                    "emailID_matched_norm": sender,
                    "emailID_matched_display": whitelist_map.get(sender, sender),
                    "SenderCandidates": "GUARANTEE_PASS",
                    "Subject": getattr(item, "Subject", ""),
                    "ReceivedTime": excel_safe_dt_str(getattr(item, "ReceivedTime", None)),
                    "FolderPath": folder.FolderPath,
                    "SavedPath": path,
                }
            )
        else:
            totals["save_failed"] += 1
            error_rows.append(
                {
                    "emailID_matched_norm": sender,
                    "SenderCandidates": "GUARANTEE_PASS",
                    "Subject": getattr(item, "Subject", ""),
                    "ReceivedTime": excel_safe_dt_str(getattr(item, "ReceivedTime", None)),
                    "FolderPath": folder.FolderPath,
                    "Error": err,
                }
            )

def guarantee_pass_for_sender_across_tree(
    root_folder,
    sender,
    whitelist_map,
    save_dir,
    only_unread,
    date_range_days,
    processed_keys,
    processed_msgids,
    stats,
    details_rows,
    error_rows,
    totals,
):
    # Root + all subfolders (stack-based)
    stack = [root_folder]
    while stack:
        folder = stack.pop()
        guarantee_pass_for_sender_in_folder(
            folder,
            sender,
            whitelist_map,
            save_dir,
            only_unread,
            date_range_days,
            processed_keys,
            processed_msgids,
            stats,
            details_rows,
            error_rows,
            totals,
        )
        for sub in folder.Folders:
            stack.append(sub)

# ============================================================
# Logging (Excel)
# ============================================================
def write_log_excel(
    log_path,
    whitelist_rows,
    whitelist_map,
    stats,
    run_meta,
    details_rows,
    error_rows,
    seen_counter,
    totals,
):
    summary = []
    norm_order = []
    seen_norm = set()
    for r in whitelist_rows:
        n = r.get("normalized", "")
        if n and n not in seen_norm:
            seen_norm.add(n)
            norm_order.append(n)
    for n in norm_order:
        b = stats.get(n, None)
        matched = (b or {}).get("matched", 0)
        saved = (b or {}).get("saved", 0)
        summary.append(
            {
                "emailID_display": whitelist_map.get(n, n),
                "emailID_normalized": n,
                "messages_found": matched,
                "messages_saved": saved,
                "no_emails": (matched == 0),
            }
        )
    df_summary = pd.DataFrame(summary)
    df_meta = pd.DataFrame([{**run_meta, **totals, "run_timestamp": excel_safe_dt_str(run_meta.get("run_timestamp"))}])
    df_details = pd.DataFrame(details_rows)
    df_errors = pd.DataFrame(error_rows)
    df_seen = pd.DataFrame([{"sender_address": k, "count": v} for k, v in seen_counter.most_common()])
    df_wlmap = pd.DataFrame(whitelist_rows)

    with pd.ExcelWriter(log_path, engine="openpyxl") as xw:
        df_summary.to_excel(xw, index=False, sheet_name="Summary")
        df_meta.to_excel(xw, index=False, sheet_name="RunMeta")
        df_wlmap.to_excel(xw, index=False, sheet_name="WhitelistMap")
        if not df_details.empty:
            df_details.to_excel(xw, index=False, sheet_name="Details")
        if not df_errors.empty:
            df_errors.to_excel(xw, index=False, sheet_name="Errors")
        if not df_seen.empty:
            df_seen.to_excel(xw, index=False, sheet_name="SendersSeen")

# ============================================================
# MAIN — Shared mailbox only
# ============================================================
def main():
    try:
        ensure_dir(SAVE_DIR)
        LOG_XLSX_PATH = (
            os.path.join(SAVE_DIR, f"ExportLog_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx")
            if LOG_XLSX is None
            else LOG_XLSX
        )

        whitelist_norm_set, whitelist_map, whitelist_rows, whitelist_all_variants = read_sender_whitelist_from_excel(EMAIL_LIST_XLSX)
        debug(f"Loaded {len(whitelist_norm_set)} sender(s) from Excel.")

        # Connect to Outlook shared mailbox
        outlook = win32.Dispatch("Outlook.Application")
        namespace = outlook.GetNamespace("MAPI")
        recipient = namespace.CreateRecipient(SHARED_MAILBOX_ADDRESS)
        if not recipient.Resolve():
            raise RuntimeError(f"Could not resolve shared mailbox '{SHARED_MAILBOX_ADDRESS}'.")
        shared_inbox = namespace.GetSharedDefaultFolder(recipient, 6)  # olFolderInbox
        if shared_inbox is None:
            raise RuntimeError("Shared Inbox not available. Verify mailbox permissions.")

        debug(f"Exporting from: {shared_inbox.Parent.Name} / {shared_inbox.Name}")
        debug(f"Saving to: {SAVE_DIR}")
        debug(
            f"GuaranteePassForAll={ENABLE_GUARANTEE_PASS_FOR_ALL_SENDERS}, "
            f"Unread={ONLY_UNREAD}, LastDays={DATE_RANGE_DAYS}, Subfolders={INCLUDE_SUBFOLDERS}"
        )
        debug(f"Log -> {LOG_XLSX_PATH}")

        processed_keys = set()
        processed_msgids = set()
        stats = {}
        details_rows = []
        error_rows = []
        seen_counter = Counter()
        totals = {
            "scanned": 0,
            "saved": 0,
            "save_failed": 0,
            "repeat_skipped": 0,
            "folders_processed": 1,
            "second_pass_saved": 0,
        }

        # Pass A: robust scan + best sender extraction through Shared Inbox (+ subfolders)
        debug("Starting Pass A: Full scan of Inbox and subfolders...")
        process_folder(
            shared_inbox,
            whitelist_norm_set=whitelist_norm_set,
            whitelist_all_variants=whitelist_all_variants,
            whitelist_map=whitelist_map,
            save_dir=SAVE_DIR,
            only_unread=ONLY_UNREAD,
            date_range_days=DATE_RANGE_DAYS,
            processed_keys=processed_keys,
            processed_msgids=processed_msgids,
            stats=stats,
            details_rows=details_rows,
            error_rows=error_rows,
            seen_counter=seen_counter,
            totals=totals,
        )
        if INCLUDE_SUBFOLDERS:
            walk_subfolders(
                shared_inbox,
                whitelist_norm_set=whitelist_norm_set,
                whitelist_all_variants=whitelist_all_variants,
                whitelist_map=whitelist_map,
                save_dir=SAVE_DIR,
                only_unread=ONLY_UNREAD,
                date_range_days=DATE_RANGE_DAYS,
                processed_keys=processed_keys,
                processed_msgids=processed_msgids,
                stats=stats,
                details_rows=details_rows,
                error_rows=error_rows,
                seen_counter=seen_counter,
                totals=totals,
            )

        # Guarantee pass for ALL senders across Shared Inbox and its subfolders
        if ENABLE_GUARANTEE_PASS_FOR_ALL_SENDERS:
            debug(f"Starting Pass B: Guarantee pass for {len(whitelist_norm_set)} senders...")
            for idx, sender in enumerate(sorted(whitelist_norm_set), 1):
                debug(f"  Checking sender {idx}/{len(whitelist_norm_set)}: {sender}")
                guarantee_pass_for_sender_across_tree(
                    shared_inbox,
                    sender,
                    whitelist_map,
                    SAVE_DIR,
                    ONLY_UNREAD,
                    DATE_RANGE_DAYS,
                    processed_keys,
                    processed_msgids,
                    stats,
                    details_rows,
                    error_rows,
                    totals,
                )

        run_meta = {
            "run_timestamp": datetime.now(),
            "save_dir": SAVE_DIR,
            "log_path": LOG_XLSX_PATH,
            "only_unread": ONLY_UNREAD,
            "date_range_days": DATE_RANGE_DAYS,
            "include_subfolders": INCLUDE_SUBFOLDERS,
            "guarantee_pass_all": ENABLE_GUARANTEE_PASS_FOR_ALL_SENDERS,
            "scan_shared_mailbox": True,
            "shared_mailbox": SHARED_MAILBOX_ADDRESS,
        }
        write_log_excel(
            LOG_XLSX_PATH,
            whitelist_rows,
            whitelist_map,
            stats,
            run_meta,
            details_rows,
            error_rows,
            seen_counter,
            totals,
        )
        debug(f"Excel log written: {LOG_XLSX_PATH}")
        debug(
            f"Done. Scanned={totals['scanned']}, Saved={totals['saved']}, "
            f"SecondPassSaved={totals['second_pass_saved']}, SaveFailed={totals['save_failed']}, "
            f"RepeatSkipped={totals['repeat_skipped']}, FoldersProcessed={totals['folders_processed']}"
        )
        return 0
    except Exception as e:
        print("Error:", e)
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    sys.exit(main())
