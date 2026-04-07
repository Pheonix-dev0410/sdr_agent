import unicodedata


def strip_accents(s: str) -> str:
    return ''.join(
        c for c in unicodedata.normalize('NFD', s)
        if unicodedata.category(c) != 'Mn'
    )


def _sanitize_name_part(name: str) -> str:
    """
    Clean a name part for use in email construction.
    - Strip accents
    - Lowercase and strip whitespace
    - Take only the FIRST word (handles compound first names like "Vivek Prakash" → "vivek")
    - Remove any remaining spaces (safety net)
    """
    cleaned = strip_accents(name.lower().strip())
    # Take first word only — middle names/compound names cause spaces in email
    first_word = cleaned.split()[0] if cleaned.split() else cleaned
    return first_word.replace(' ', '')


def construct_email(first_name: str, last_name: str, email_format: str, domain: str) -> str:
    first = _sanitize_name_part(first_name)
    last = _sanitize_name_part(last_name)
    f = first[0] if first else ''
    l = last[0] if last else ''

    format_map = {
        'firstname.lastname':  f'{first}.{last}@{domain}',
        'firstname_lastname':  f'{first}_{last}@{domain}',
        'firstnamelastname':   f'{first}{last}@{domain}',
        'firstname':           f'{first}@{domain}',
        'flastname':           f'{f}{last}@{domain}',
        'f.lastname':          f'{f}.{last}@{domain}',
        'firstnamel':          f'{first}{l}@{domain}',
        'firstname.l':         f'{first}.{l}@{domain}',
        'lastname.firstname':  f'{last}.{first}@{domain}',
        'lastname_firstname':  f'{last}_{first}@{domain}',
        'lastnamefirstname':   f'{last}{first}@{domain}',
        'lastname':            f'{last}@{domain}',
        'lfirstname':          f'{l}{first}@{domain}',
        'l.firstname':         f'{l}.{first}@{domain}',
        'firstname-lastname':  f'{first}-{last}@{domain}',
        'lastname-firstname':  f'{last}-{first}@{domain}',
        'f_lastname':          f'{f}_{last}@{domain}',
        'fl':                  f'{f}{l}@{domain}',
    }

    return format_map.get(email_format, f'{first}.{last}@{domain}')


FALLBACK_FORMATS = [
    'firstname.lastname',
    'flastname',
    'firstname',
    'firstnamelastname',
    'lastname.firstname',
]


def get_fallback_emails(first_name: str, last_name: str, domain: str) -> list[str]:
    seen = set()
    emails = []
    for fmt in FALLBACK_FORMATS:
        email = construct_email(first_name, last_name, fmt, domain)
        if email not in seen:
            seen.add(email)
            emails.append(email)
    return emails
