class WrongDataException(Exception):
    def __init__(self, message):
        self.message = "The data '%s' is not valid" % message
        super(WrongDataException, self).__init__(message)


class HTTPException(Exception):
    def __init__(self, message):
        self.message = "%s" % message
        super(HTTPException, self).__init__(message)


class UnexpectedBehaviour(Exception):
    pass


class OperatorActionRequired(Exception):
    def __init__(self, message):
        self.message = "Operator action required to complete method. See --help for futher information. %s" % message
        super(OperatorActionRequired, self).__init__(self.message)


class MaxTriesExceeded(Exception):
    def __init__(self, curTries, maxTries, message=None):
        self.message = "Max retries %d exceeded, current %d. %s" % (int(maxTries), int(curTries), message)
        super(MaxTriesExceeded, self).__init__(self.message)
