import meowmx


class CatCreated(meowmx.Event):
    cat_name: str


class CatUpdated(meowmx.Event):
    new_random_value: str
    version: int
