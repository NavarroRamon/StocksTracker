import os

def read_value(file_path: str) -> str | None:
    """Lee el valor almacenado en el fichero.
    Si no existe, devuelve None."""
    if not os.path.exists(file_path):
        return None
    with open(file_path, "r", encoding="utf-8") as f:
        value = f.read().strip()
    return value or None


def write_value(file_path: str, value: str):
    """Crea o sobrescribe el fichero con el nuevo valor."""
    os.makedirs('data', exist_ok=True)
    with open(file_path, "w", encoding="utf-8") as f:
        f.write(str(value))


def delete_value(file_path: str):
    """Elimina el fichero si existe (reinicia el estado)."""
    try:
        os.remove(file_path)
    except FileNotFoundError:
        pass