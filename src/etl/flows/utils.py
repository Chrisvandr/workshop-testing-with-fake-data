from pydantic import ValidationError
from sqlmodel import SQLModel
from structlog import get_logger

logger = get_logger(__name__)


def collect_validation_errors(  # noqa: C901
    model: SQLModel,
    visited: set | None = None,
    errors: list | None = None,
    **kwargs,
) -> list[ValidationError]:
    """
    Validate a SQLModel object and its relationships through recursion.

    The `model_validate` method validates the model's fields but does not
    automatically validate relationships. This function ensures that all
    related models are also validated.

    The `visited` parameter is used to keep track of already visited models
    to avoid infinite recursion in case of circular relationships.
    """
    if errors is None:
        errors = []
    if visited is None:
        visited = set()

    # Use the model's id to uniquely identify it
    model_id = id(model)
    if model_id in visited:
        return []
    visited.add(model_id)

    try:
        model.__class__.model_validate(model.__dict__, **kwargs)
    except ValidationError as err:
        logger.error(
            f"Error validating {model.__class__.__name__}: {err} \n"
            f"Model data: {dict(model)} \n",
        )
        errors.append(err)

    for rel_name in model.__sqlmodel_relationships__:
        value = getattr(model, rel_name)
        if isinstance(value, SQLModel):
            errors += collect_validation_errors(value, visited, errors, **kwargs)
        elif isinstance(value, list) and all(
            isinstance(item, SQLModel) for item in value
        ):
            for item in value:
                errors += collect_validation_errors(item, visited, errors, **kwargs)

    return errors
