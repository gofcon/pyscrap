import json

from sqlalchemy.ext.compiler import compiles
from sqlalchemy.types import Boolean, Text, TypeDecorator


class JSONText(TypeDecorator):
    """Native Oracle JSON column (Oracle 21c+, verified against 26ai).

    SQLAlchemy's oracle dialect has no DDL support for the generic JSON type
    (no ``visit_JSON`` on its type compiler), and the generic type's default
    bind/result processors assume a dialect-level ``_json_serializer`` that
    the oracle dialect doesn't provide either. So this type renders the
    column as native Oracle ``JSON`` itself (via ``@compiles``) and handles
    Python-side (de)serialization explicitly.

    On read, python-oracledb already deserializes native JSON columns into
    Python dict/list for us (numbers come back as ``decimal.Decimal`` to
    avoid precision loss), so ``process_result_value`` only needs to cover
    the fallback case where a raw string is returned.
    """

    impl = Text
    cache_ok = True

    def process_bind_param(self, value, dialect):
        if value is None:
            return None
        return json.dumps(value)

    def process_result_value(self, value, dialect):
        if value is None:
            return None
        if isinstance(value, (dict, list)):
            return value
        return json.loads(value)


@compiles(JSONText, "oracle")
def _compile_json_type_oracle(element, compiler, **kw):
    return "JSON"


class XMLText(TypeDecorator):
    """Native Oracle XMLTYPE column.

    Like JSONText, this exists because SQLAlchemy's oracle dialect has no
    built-in XMLTYPE support, so the DDL is rendered manually via
    ``@compiles``. Unlike JSON, python-oracledb handles XMLTYPE bind/fetch
    transparently as plain ``str`` in both directions, so no
    process_bind_param/process_result_value override is needed here.
    """

    impl = Text
    cache_ok = True


@compiles(XMLText, "oracle")
def _compile_xml_type_oracle(element, compiler, **kw):
    return "XMLTYPE"


class OracleBoolean(TypeDecorator):
    """Native Oracle BOOLEAN column (Oracle 23c+, verified against 26ai).

    SQLAlchemy's oracle dialect predates Oracle's native BOOLEAN column type
    and maps the generic Boolean type to NUMBER(1) (0/1) instead, so this
    renders true BOOLEAN DDL via ``@compiles``. python-oracledb already
    round-trips Python True/False <-> BOOLEAN natively, so no
    process_bind_param/process_result_value override is needed.
    """

    impl = Boolean
    cache_ok = True


@compiles(OracleBoolean, "oracle")
def _compile_oracle_boolean(element, compiler, **kw):
    return "BOOLEAN"
