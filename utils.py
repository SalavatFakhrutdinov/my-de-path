def normalize_name(input_str: str) -> str:
    if not isinstance(input_str, str):
        print(f"Warning: expected string, returned {type(input_str)}")
        return ""
    return " ".join(input_str.split()).lower()
    