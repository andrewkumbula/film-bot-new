# Дымовые тесты: импорт модулей приложения не падает.

def test_import_config():
    from app.config import load_settings
    s = load_settings()
    assert s.bot_token
    assert s.openrouter_api_key
    assert str(s.db_path).endswith(".db")


def test_import_db():
    from app.db import database
    assert hasattr(database, "init_db")


def test_import_user_settings():
    from app.services import user_settings
    assert hasattr(user_settings, "get_min_rating_filter_enabled")
    assert hasattr(user_settings, "set_min_rating_filter")
    assert user_settings.passes_min_rating_filter(7.0, True) is True
    assert user_settings.passes_min_rating_filter(5.0, True) is False
    assert user_settings.passes_min_rating_filter(None, True) is True
