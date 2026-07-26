import { Injectable } from '@angular/core';
import { Subject } from 'rxjs';

/**
 * Servicio de grabación activado por voz (VAD) y reconocimiento nativo del navegador.
 */
@Injectable({ providedIn: 'root' })
export class AudioRecorderService {

  private readonly _utteranceReady = new Subject<Blob>();
  readonly onUtteranceReady$ = this._utteranceReady.asObservable();

  /** Emite el texto parcial reconocido por el navegador */
  private readonly _partialTranscript = new Subject<string>();
  readonly onPartialTranscript$ = this._partialTranscript.asObservable();

  /** Emite fragmentos finales de oraciones para intérprete simultáneo */
  private readonly _finalChunk = new Subject<string>();
  readonly onFinalChunk$ = this._finalChunk.asObservable();

  /** Emite el nivel de volumen normalizado (0–1) en cada frame de audio */
  private readonly _volumeLevel = new Subject<number>();
  readonly onVolumeLevel$ = this._volumeLevel.asObservable();

  // ─── Estado interno ────────────────────────────────────────────────────────
  private _stream: MediaStream | null = null;
  private mediaRecorder: MediaRecorder | null = null;
  private audioChunks: Blob[] = [];
  private recognition: any = null;

  // Web Audio — análisis de volumen
  private audioContext: AudioContext | null = null;
  private analyser: AnalyserNode | null = null;
  private animationFrameId: number | null = null;
  private silenceTimer: ReturnType<typeof setTimeout> | null = null;
  private hasSpeech = false;
  private speechStartTime = 0;
  private isStopping = false;
  private finalTranscript = '';

  // ─── Configuración ─────────────────────────────────────────────────────────
  /** Umbral RMS (0.015 = equilibrio perfecto entre sensibilidad y rechazo de ruido) */
  private readonly SILENCE_THRESHOLD   = 0.015;
  /** Tiempo de silencio tras voz que dispara el fin de frase (500ms = muy rápido) */
  private readonly SILENCE_DURATION_MS = 500;

  public isMuted = false;

  constructor() {}

  /**
   * Mutea temporalmente la grabación y el reconocimiento.
   * Ideal para evitar que el micrófono capte el audio del TTS.
   */
  setMuted(muted: boolean) {
    this.isMuted = muted;
    if (muted) {
      if (this.recognition) {
        try { this.recognition.abort(); } catch (e) {}
      }
      if (this.mediaRecorder && this.mediaRecorder.state === 'recording') {
        this.mediaRecorder.pause();
      }
    } else {
      if (this.recognition) {
        try { this.recognition.start(); } catch (e) {}
      }
      if (this.mediaRecorder && this.mediaRecorder.state === 'paused') {
        this.mediaRecorder.resume();
      }
    }
  }

  // ─── API pública ───────────────────────────────────────────────────────────

  async startRecording(langCode: string = 'es'): Promise<void> {
    this._stream = await navigator.mediaDevices.getUserMedia({
      audio: {
        echoCancellation: true,
        noiseSuppression: true,
        autoGainControl: true,
      }
    });

    this.audioChunks = [];
    this.hasSpeech   = false;
    this.isStopping  = false;
    this.finalTranscript = '';

    this._startBrowserRecognition(langCode);
    this._createAndStartMediaRecorder();
    this._startSilenceDetection(this._stream);
  }

  async fullyStop(): Promise<void> {
    this.isStopping = true;
    this._stopSilenceDetection();
    await new Promise<void>(resolve => {
      if (!this.mediaRecorder || this.mediaRecorder.state === 'inactive') {
        resolve();
      } else {
        this.mediaRecorder.onstop = () => resolve();
        this.mediaRecorder.stop();
      }
    });
    this._stopBrowserRecognition();
    this._releaseStream();
    this._volumeLevel.next(0);
  }

  // ─── Internos ──────────────────────────────────────────────────────────────

  private _createAndStartMediaRecorder(): void {
    if (!this._stream) return;

    this.mediaRecorder = new MediaRecorder(this._stream, { mimeType: 'audio/webm' });

    this.mediaRecorder.ondataavailable = (e: BlobEvent) => {
      if (e.data.size > 0) {
        this.audioChunks.push(e.data);
      }
    };

    this.mediaRecorder.onstop = () => {
      if (this.audioChunks.length > 0 && this.hasSpeech) {
        const blob = new Blob(this.audioChunks, { type: 'audio/webm' });
        this._utteranceReady.next(blob);
      }

      this.audioChunks = [];
      this.hasSpeech   = false;
      this.finalTranscript = '';

      // Reinicio automático para la siguiente frase
      if (!this.isStopping) {
        this._createAndStartMediaRecorder();
      }
    };

    this.mediaRecorder.start();
  }

  private _startBrowserRecognition(langCode: string): void {
    const SpeechRecognition = (window as any).SpeechRecognition || (window as any).webkitSpeechRecognition;
    if (!SpeechRecognition) return;

    this.recognition = new SpeechRecognition();
    this.recognition.continuous = true;
    this.recognition.interimResults = true;
    this.recognition.lang = langCode === 'es' ? 'es-ES' : (langCode === 'en' ? 'en-US' : langCode);

    this.recognition.onresult = (event: any) => {
      if (this.isMuted) return;
      let interimTranscript = '';
      for (let i = event.resultIndex; i < event.results.length; i++) {
        const chunk = event.results[i][0].transcript;
        if (event.results[i].isFinal) {
          this.finalTranscript += chunk + ' ';
          this._finalChunk.next(chunk.trim());
        } else {
          interimTranscript += chunk;
        }
      }
      this._partialTranscript.next((this.finalTranscript + interimTranscript).trim());
    };
    
    this.recognition.onend = () => {
      if (!this.isStopping && this.recognition && !this.isMuted) {
        try { this.recognition.start(); } catch (e) {}
      }
    };
    
    try {
      this.recognition.start();
    } catch (e) {}
  }

  private _stopBrowserRecognition(): void {
    if (this.recognition) {
      try { this.recognition.stop(); } catch (e) {}
      this.recognition = null;
    }
  }

  private _startSilenceDetection(stream: MediaStream): void {
    this.audioContext = new AudioContext();
    const source = this.audioContext.createMediaStreamSource(stream);
    this.analyser  = this.audioContext.createAnalyser();
    this.analyser.fftSize = 2048;
    source.connect(this.analyser);

    const dataArray = new Float32Array(this.analyser.fftSize);

    const processAudio = () => {
      if (this.isStopping) return;
      if (this.isMuted) {
        requestAnimationFrame(processAudio);
        return;
      }
      this.analyser!.getFloatTimeDomainData(dataArray);

      // Calcular RMS (energía de la señal de audio)
      let sum = 0;
      for (let i = 0; i < dataArray.length; i++) sum += dataArray[i] * dataArray[i];
      const rms = Math.sqrt(sum / dataArray.length);

      // Emitir nivel de volumen para la animación de ondas (normalizado 0..1)
      const normalized = Math.min(rms / 0.15, 1);
      this._volumeLevel.next(normalized);

      if (rms >= this.SILENCE_THRESHOLD) {
        // Voz detectada
        if (!this.hasSpeech) {
          this.hasSpeech = true;
          this.speechStartTime = Date.now();
        }
        if (this.silenceTimer) {
          clearTimeout(this.silenceTimer);
          this.silenceTimer = null;
        }
      } else {
        if (this.hasSpeech && !this.silenceTimer) {
          this.silenceTimer = setTimeout(() => {
            if (this.mediaRecorder && this.mediaRecorder.state === 'recording') {
              this.mediaRecorder.stop();
            }
            if (this.recognition) {
              try { this.recognition.abort(); } catch (e) {}
            }
            this.silenceTimer = null;
          }, this.SILENCE_DURATION_MS);
        }
      }

      this.animationFrameId = requestAnimationFrame(processAudio);
    };

    this.animationFrameId = requestAnimationFrame(processAudio);
  }

  private _stopSilenceDetection(): void {
    if (this.animationFrameId !== null) { cancelAnimationFrame(this.animationFrameId); this.animationFrameId = null; }
    if (this.silenceTimer)              { clearTimeout(this.silenceTimer); this.silenceTimer = null; }
    if (this.audioContext)              { this.audioContext.close().catch(() => {}); this.audioContext = null; }
    this.analyser = null;
  }

  private _releaseStream(): void {
    this._stream?.getTracks().forEach(t => t.stop());
    this._stream     = null;
    this.mediaRecorder = null;
  }
}
