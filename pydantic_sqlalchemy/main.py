from typing import Any, Container, Optional, Tuple, Type, Union

from pydantic import BaseConfig, BaseModel, Field, create_model
from pydantic.fields import FieldInfo
from sqlalchemy import Column
from sqlalchemy.inspection import inspect
from sqlalchemy.orm.properties import ColumnProperty
from sqlalchemy.sql import Select
from sqlalchemy.sql.elements import Label


class OrmConfig(BaseConfig):
    orm_mode = True


def _columns(sql: Select):
    # sqlalchemy 2.0 will use selected_columns
    if hasattr(sql, "selected_columns"):
        return sql.selected_columns  # type: ignore

    if hasattr(sql, "columns"):
        return sql.columns  # type: ignore

    raise TypeError("Could not found columns")


def __column_to_field(column: Union[Column, Label]) -> Tuple[Any, FieldInfo]:
    default = None
    python_type: Optional[type] = None

    impl = getattr(column.type, "impl", None)
    if impl:
        python_type = getattr(impl, "python_type", None)
    elif hasattr(column.type, "python_type"):
        python_type = column.type.python_type

    assert python_type, f"Could not infer python_type for {column}"

    if isinstance(column, Column) and column.default is None and not column.nullable:
        default = ...

    comment = getattr(column, "comment", None)

    return (
        python_type,
        Field(description=str(comment) if comment else None, default=default),
    )


def sqlalchemy_to_pydantic(
    db_model: Type, *, config: Type = OrmConfig, exclude: Container[str] = [],
) -> Type[BaseModel]:
    mapper = inspect(db_model)
    fields = {}
    for attr in mapper.attrs:
        if isinstance(attr, ColumnProperty):
            if attr.columns:
                name = attr.key
                if name in exclude:
                    continue
                column = attr.columns[0]
                fields[name] = __column_to_field(column)
    pydantic_model = create_model(
        db_model.__name__, __config__=config, **fields  # type: ignore
    )
    return pydantic_model


def sqlalchemy_select_to_pydantic(
    module_name: str, sql: Select, *, config: Type = OrmConfig,
) -> Type[BaseModel]:
    fields = {}
    # sqlalchemy 2.0 will use selected_columns
    columns = _columns(sql)

    for column in columns:
        # sql function has not column name
        if not isinstance(column, (Column, Label)):
            continue

        name = str(column.key)
        fields[name] = __column_to_field(column)
    pydantic_model = create_model(module_name, __config__=config, **fields)
    return pydantic_model
