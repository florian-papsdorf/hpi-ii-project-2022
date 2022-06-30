class ContinuousIDGenerator:
    def __init__(self, prefix: str):
        self.counter = 0
        self.prefix = prefix

    def get_next_identifier(self) -> str:
        next_identifier = f'{self.prefix}-{self.counter}'
        self.counter += 1
        return next_identifier
