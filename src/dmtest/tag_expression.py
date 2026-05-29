class ParseError(Exception):
    pass


def _tokenize(expr):
    tokens = []
    i = 0
    while i < len(expr):
        if expr[i].isspace():
            i += 1
        elif expr[i] == '(':
            tokens.append('(')
            i += 1
        elif expr[i] == ')':
            tokens.append(')')
            i += 1
        else:
            j = i
            while j < len(expr) and not expr[j].isspace() and expr[j] not in '()':
                j += 1
            tokens.append(expr[i:j])
            i = j
    return tokens


def _parse(tokens):
    pos = [0]

    def peek():
        if pos[0] < len(tokens):
            return tokens[pos[0]]
        return None

    def consume():
        token = tokens[pos[0]]
        pos[0] += 1
        return token

    def parse_or():
        left = parse_and()
        while peek() == 'or':
            consume()
            right = parse_and()
            left_capture = left
            right_capture = right
            left = lambda tags, l=left_capture, r=right_capture: l(tags) or r(tags)
        return left

    def parse_and():
        left = parse_not()
        while peek() == 'and':
            consume()
            right = parse_not()
            left_capture = left
            right_capture = right
            left = lambda tags, l=left_capture, r=right_capture: l(tags) and r(tags)
        return left

    def parse_not():
        if peek() == 'not':
            consume()
            operand = parse_not()
            return lambda tags, o=operand: not o(tags)
        return parse_atom()

    def parse_atom():
        token = peek()
        if token is None:
            raise ParseError("unexpected end of expression")
        if token == '(':
            consume()
            result = parse_or()
            if peek() != ')':
                raise ParseError("expected ')'")
            consume()
            return result
        if token in ('and', 'or', ')'):
            raise ParseError(f"unexpected '{token}'")
        consume()
        return lambda tags, t=token: t in tags

    result = parse_or()
    if pos[0] < len(tokens):
        raise ParseError(f"unexpected '{tokens[pos[0]]}' after expression")
    return result


def parse_tag_expression(expr_str):
    """Parse a boolean tag expression and return a matcher function.

    The matcher takes a frozenset of tags and returns True if the
    tags satisfy the expression.

    Examples:
        "smoke"                          -> has tag 'smoke'
        "not benchmark"                  -> does not have tag 'benchmark'
        "smoke and not experimental"     -> has smoke, not experimental
        "validation or functional"       -> has either
        "not (benchmark or experimental)" -> has neither
    """
    expr_str = expr_str.strip()
    if not expr_str:
        raise ParseError("empty tag expression")
    tokens = _tokenize(expr_str)
    return _parse(tokens)
