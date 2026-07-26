"""
Servicio de traducción de texto rápido usando deep-translator.
"""

from deep_translator import GoogleTranslator

def translate(text: str, source_lang: str, target_lang: str) -> str:
    """
    Traduce un texto de un idioma a otro de forma rápida.
    
    Args:
        text: Texto a traducir.
        source_lang: Código ISO 639-1 (ej. "es").
        target_lang: Código ISO 639-1 (ej. "en").
    """
    try:
        translator = GoogleTranslator(source=source_lang, target=target_lang)
        return translator.translate(text)
    except Exception as e:
        print(f"[Translation] Error con deep-translator: {e}")
        return "[Error en la traducción]"
