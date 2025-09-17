def create_historical_schema_from_model(pydantic_model):
    """Tự động tạo dictionary schema cho historical processor từ Pydantic model."""
    schema = {}
    type_mapping = {
        'datetime': 'datetime64[ns, UTC]', 'date': 'datetime64[ns, UTC]',
        'int': 'Int64', 'float': 'float64', 'bool': 'boolean'
    }
    for field_name, field in pydantic_model.model_fields.items():
        type_str = str(field.annotation)
        mapped_type = "object"
        for py_type, pd_type in type_mapping.items():
            if py_type in type_str:
                mapped_type = pd_type
                break
        schema[field_name] = mapped_type
    schema.update({
        "extracted_at": "datetime64[ns, UTC]", "valid_from": "datetime64[ns, UTC]",
        "valid_to": "datetime64[ns, UTC]", "is_current": "boolean", "branch_id": "Int64"
    })
    return schema