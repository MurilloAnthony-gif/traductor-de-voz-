import { Component, inject, signal, computed, OnDestroy, ElementRef, ViewChild, NgZone } from '@angular/core';
import { Router } from '@angular/router';
import { Subscription } from 'rxjs';
import { debounceTime } from 'rxjs/operators';
import { ButtonModule } from 'primeng/button';
import { CardModule } from 'primeng/card';
import { TagModule } from 'primeng/tag';
import { SelectModule } from 'primeng/select';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { ProgressSpinnerModule } from 'primeng/progressspinner';
import { RatingModule } from 'primeng/rating';

import { AudioRecorderService } from './services/audio-recorder.service';
import { WebsocketTranslatorService } from './services/websocket-translator.service';
import { RecordingState, WsMessage, Language } from './models/translation-result.model';

@Component({
  selector: 'app-voice-translator',
  imports: [
    CommonModule, FormsModule,
    ButtonModule, CardModule, TagModule, SelectModule,
    ProgressSpinnerModule, RatingModule,
  ],
  templateUrl: './voice-translator.html',
  styleUrl: './voice-translator.scss',
})
export class VoiceTranslator implements OnDestroy {
  @ViewChild('waveCanvas') waveCanvasRef!: ElementRef<HTMLCanvasElement>;

  private router       = inject(Router);
  private audioRecorder = inject(AudioRecorderService);
  private wsTranslator  = inject(WebsocketTranslatorService);
  private ngZone        = inject(NgZone);

  // ─── Estado reactivo ───────────────────────────────────────────────────────
  readonly state         = signal<RecordingState>('idle');
  readonly transcripcion = signal<string>('');
  readonly traduccion    = signal<string>('');
  readonly errorMessage  = signal('');
  readonly detectedLang  = signal<string | null>(null);
  readonly resultTarget  = signal<string | null>(null);

  ratingValue = 0;
  private subs: Subscription[] = [];

  // Variables de audio para reproducción secuencial (intérprete simultáneo)
  private currentAudio: HTMLAudioElement | null = null;
  private audioQueue: string[] = [];
  private isPlayingQueue = false;
  private readonly WAVE_BARS = 32;

  // Animación de ondas
  private volumeLevel = 0;
  private waveAnimId: number | null = null;

  // ─── Configuración de Idiomas ──────────────────────────────────────────────
  readonly languages: Language[] = [
    { name: 'Español', code: 'es', flag: 'es' },
    { name: 'Inglés', code: 'en', flag: 'en' },
    { name: 'Francés', code: 'fr', flag: 'fr' },
    { name: 'Alemán', code: 'de', flag: 'de' },
  ];

  sourceLang: Language = this.languages[0];
  targetLang: Language = this.languages[1];

  // ─── Computed ──────────────────────────────────────────────────────────────
  readonly isIdle       = computed(() => this.state() === 'idle');
  readonly isListening  = computed(() => this.state() === 'listening');
  readonly isProcessing = computed(() => this.state() === 'processing');
  readonly isError      = computed(() => this.state() === 'error');

  readonly micMainLabel = computed(() =>
    this.isIdle() ? 'Iniciar escucha' : 'Detener'
  );

  readonly micSubLabel = computed(() => {
    if (this.isListening()) {
      return `Habla en ${this.sourceLang.name} o ${this.targetLang.name}...`;
    }
    if (this.isProcessing()) return 'Traduciendo y generando voz...';
    return '';
  });

  readonly inputStatusLabel = computed(() =>
    ({ idle: 'En espera', listening: 'Escuchando', processing: 'Procesando', done: 'Listo', error: 'Error' }[this.state()])
  );

  readonly inputStatusSeverity = computed<'secondary' | 'warn' | 'success' | 'danger'>(() =>
    ({ idle: 'secondary', listening: 'warn', processing: 'warn', done: 'success', error: 'danger' }[this.state()] as any)
  );

  // ─── Acciones públicas ─────────────────────────────────────────────────────

  async toggleRecording(): Promise<void> {
    if (this.isIdle() || this.isError()) {
      await this.startListening();
    } else {
      await this.stopAll();
    }
  }

  swapLanguages(): void {
    if (!this.isIdle()) return;
    [this.sourceLang, this.targetLang] = [this.targetLang, this.sourceLang];
    this.onConfigChange();
  }

  onConfigChange(): void {
    // Envia la config al backend inmediatamente (si está conectado)
    this.wsTranslator.sendConfig('manual', this.sourceLang.code, this.targetLang.code);
  }

  // ─── Internos ──────────────────────────────────────────────────────────────

  private async startListening(): Promise<void> {
    this.resetResults();
    this.state.set('listening');

    try {
      this.wsTranslator.connect();
      // Esperar brevemente a que conecte para mandar config inicial
      setTimeout(() => this.onConfigChange(), 500);

      this._subscribeToWsMessages();

      await this.audioRecorder.startRecording(this.sourceLang.code);

      this.subs.push(
        this.audioRecorder.onVolumeLevel$.subscribe(vol => {
          this.volumeLevel = vol;
        })
      );

      this.subs.push(
        this.audioRecorder.onPartialTranscript$.subscribe((text) => {
          this.transcripcion.set(text);
        })
      );

      this.subs.push(
        this.audioRecorder.onPartialTranscript$.pipe(
          debounceTime(500)
        ).subscribe((text) => {
          this.wsTranslator.send({ type: 'translate_text', text: text });
        })
      );

      this.subs.push(
        this.audioRecorder.onFinalChunk$.subscribe((chunk) => {
          this.wsTranslator.send({ type: 'translate_and_speak_chunk', text: chunk });
        })
      );

      this.subs.push(
        this.audioRecorder.onUtteranceReady$.subscribe((blob) => {
          this.ngZone.run(() => this.state.set('processing'));
          this.wsTranslator.sendEndUtterance(blob);
        })
      );

      this._startWaveAnimation();

    } catch (err) {
      this.setError('No se pudo acceder al micrófono. Por favor, revisa los permisos.');
    }
  }

  private _subscribeToWsMessages(): void {
    this.subs.push(
      this.wsTranslator.messages$.subscribe((msg: WsMessage) => {
        switch (msg.type) {
          case 'partial_translation_result':
            if ((msg as any).traduccion) {
              this.traduccion.set((msg as any).traduccion);
            }
            break;

          case 'partial_audio':
            if ((msg as any).audio_base64) {
              this.queueAudio((msg as any).audio_base64);
            }
            break;

          case 'translation_result':
            this.transcripcion.set(msg.transcripcion);
            this.traduccion.set(msg.traduccion);
            this.detectedLang.set(msg.source_lang ?? null);
            this.resultTarget.set(msg.target_lang ?? null);
            this.state.set('listening');
            this.playAudio(msg.audio_base64);
            break;

          case 'no_speech':
            this.errorMessage.set(msg.message);
            this.state.set('listening');
            setTimeout(() => this.errorMessage.set(''), 4000);
            break;

          case 'error':
            this.errorMessage.set(msg.message ?? 'Error desconocido.');
            this.state.set('listening');
            setTimeout(() => this.errorMessage.set(''), 5000);
            break;
        }
      })
    );
  }

  // ─── Reproducción de Audio (Cola y Final) ──────────────────────────────

  private queueAudio(base64: string): void {
    this.audioQueue.push(base64);
    this.playNextAudio();
  }

  private playNextAudio(): void {
    if (this.isPlayingQueue || this.audioQueue.length === 0) return;
    this.isPlayingQueue = true;
    
    const base64 = this.audioQueue.shift();
    if (!base64) {
      this.isPlayingQueue = false;
      return;
    }

    this.currentAudio = new Audio('data:audio/mp3;base64,' + base64);
    this.currentAudio.onended = () => {
      this.isPlayingQueue = false;
      this.playNextAudio();
    };
    this.currentAudio.play().catch(e => {
      console.error('Error reproduciendo chunk de audio:', e);
      this.isPlayingQueue = false;
      this.playNextAudio();
    });
  }

  private playAudio(base64: string): void {
    // Al reproducir el audio final completo, vaciamos la cola parcial
    this.audioQueue = [];
    if (this.currentAudio) {
      this.currentAudio.pause();
    }
    this.currentAudio = new Audio('data:audio/mp3;base64,' + base64);
    this.currentAudio.onended = () => { this.isPlayingQueue = false; };
    this.isPlayingQueue = true;
    this.currentAudio.play().catch(e => console.error('Error audio final:', e));
  }

  private async stopAll(): Promise<void> {
    this._stopWaveAnimation();
    this.subs.forEach(sub => sub.unsubscribe());
    this.subs = [];
    await this.audioRecorder.fullyStop();
    this.wsTranslator.disconnect();
    this.state.set('idle');
    this.volumeLevel = 0;
    this._clearCanvas();
  }

  // ─── Animación de Ondas ────────────────────────────────────────────────────

  private _startWaveAnimation(): void {
    setTimeout(() => this._drawWave(), 100);
  }

  private _drawWave(): void {
    if (!this.waveCanvasRef) return;
    const canvas = this.waveCanvasRef.nativeElement;
    const ctx = canvas.getContext('2d');
    if (!ctx) return;

    const draw = () => {
      const W = canvas.width;
      const H = canvas.height;
      ctx.clearRect(0, 0, W, H);

      const barWidth = 4;
      const gap = (W - this.WAVE_BARS * barWidth) / (this.WAVE_BARS + 1);
      const vol = this.volumeLevel;
      const isActive = this.isListening() || this.isProcessing();

      for (let i = 0; i < this.WAVE_BARS; i++) {
        const x = gap + i * (barWidth + gap);
        const phase = (Date.now() / 150 + i * 0.4);
        const sinVal = Math.abs(Math.sin(phase));
        const minH = 4;
        const maxH = H * 0.85;

        let barH: number;
        if (!isActive) {
          barH = minH;
        } else if (vol < 0.02) {
          barH = minH + sinVal * (H * 0.12);
        } else {
          barH = minH + sinVal * vol * (maxH - minH);
        }

        const y = (H - barH) / 2;

        const grad = ctx.createLinearGradient(0, y, 0, y + barH);
        if (this.isProcessing()) {
          grad.addColorStop(0, '#f59e0b');
          grad.addColorStop(1, '#d97706');
        } else if (isActive) {
          grad.addColorStop(0, '#6366f1');
          grad.addColorStop(1, '#3b82f6');
        } else {
          grad.addColorStop(0, '#cbd5e1');
          grad.addColorStop(1, '#94a3b8');
        }

        ctx.fillStyle = grad;
        ctx.beginPath();
        ctx.roundRect(x, y, barWidth, barH, 2);
        ctx.fill();
      }

      this.waveAnimId = requestAnimationFrame(draw);
    };

    this.waveAnimId = requestAnimationFrame(draw);
  }

  private _stopWaveAnimation(): void {
    if (this.waveAnimId !== null) {
      cancelAnimationFrame(this.waveAnimId);
      this.waveAnimId = null;
    }
  }

  private _clearCanvas(): void {
    if (!this.waveCanvasRef) return;
    const canvas = this.waveCanvasRef.nativeElement;
    const ctx = canvas.getContext('2d');
    ctx?.clearRect(0, 0, canvas.width, canvas.height);
  }

  private resetResults(): void {
    this.transcripcion.set('');
    this.traduccion.set('');
    this.errorMessage.set('');
    this.detectedLang.set(null);
    this.resultTarget.set(null);
  }

  private setError(message: string): void {
    this.errorMessage.set(message);
    this.state.set('error');
  }

  ngOnDestroy(): void { this.stopAll(); }

  cerrarSesion(): void { this.router.navigate(['/login']); }
  verPlanes(): void { this.router.navigate(['/planes']); }
}
