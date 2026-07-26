export interface WsTranslationResult {
  type: 'translation_result';
  transcripcion: string;
  traduccion: string;
  audio_base64: string;
  source_lang?: string;
  target_lang?: string;
}

export interface WsMeetingResult {
  type: 'meeting_result';
  transcripcion: string;
  traduccion: string;
  audio_base64: string;
  source_lang?: string;
  target_lang?: string;
}

export interface WsNoSpeech {
  type: 'no_speech';
  message: string;
}

export interface WsError {
  type: 'error';
  message: string;
}

export interface WsPartialTranscription {
  type: 'partial_transcription';
  text: string;
}

export interface WsPartialTranslationResult {
  type: 'partial_translation_result';
  traduccion: string;
  emocion?: string;
}

export interface WsPartialAudio {
  type: 'partial_audio';
  audio_base64: string;
}

export type WsMessage = WsTranslationResult | WsMeetingResult | WsPartialTranscription | WsPartialTranslationResult | WsPartialAudio | WsNoSpeech | WsError;

/** Estados posibles del flujo de grabación y traducción. */
export type RecordingState = 'idle' | 'listening' | 'meeting' | 'processing' | 'error';

/** Idiomas soportados en el prototipo. */
export interface Language {
  name: string;
  code: string;
  flag: string;
}
