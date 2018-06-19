
class WrongDataException(Exception):
    def __init__(self, d):
        message = "The data '%s' is not valid" % str(d)
        super(WrongDataException, self).__init__(message)


class HTTPException(Exception):
    def __init__(self, d):
        message = "%s" % str(d)
        super(HTTPException, self).__init__(message)


class UnexpectedBehaviour(Exception):
    pass


class OperatorActionRequired(Exception):
    def __init__(self, d):
        message = "Operator action required to complete method. See --help for futher information. %s" % str(d)
        super(OperatorActionRequired, self).__init__(message)


class MaxTriesExceeded(Exception):
    def __init__(self, curTries, maxTries, msg=None):
        message = "Max retries %d exceeded, current %d. %s" % (int(maxTries), int(curTries), msg)
        super(MaxTriesExceeded, self).__init__(message)