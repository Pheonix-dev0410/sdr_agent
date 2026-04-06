import unicodedata


def strip_accents(s: str) -> str:
    return ''.join(
        c for c in unicodedata.normalize('NFD', s)
        if unicodedata.category(c) != 'Mn'
    )


def construct_email(first_name: str, last_name: str, email_format: str, domain: str) -> str:
    first = strip_accents(first_name.lower().strip())
    last = strip_accents(last_name.lower().strip())
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
