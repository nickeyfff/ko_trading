def generate_symbol(code):
    if code.startswith(("00", "30")):
        return "sz" + code
    elif code.startswith(("60", "68")):
        return "sh" + code
    elif code.startswith(("92")):
        return "bj" + code
    else:
        return code
