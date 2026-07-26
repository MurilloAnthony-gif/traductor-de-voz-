import { Injectable } from '@angular/core';
import { Subject, Observable } from 'rxjs';
import { WsMessage } from '../models/translation-result.model';

/**
 * Servicio encargado de gestionar la comunicación bidireccional
 * en tiempo real con el servidor FastAPI mediante WebSockets.
 */
@Injectable({ providedIn: 'root' })
export class WebsocketTranslatorService {
  private ws: WebSocket | null = null;

  // Subjects para emitir eventos recibidos del servidor
  private readonly _messages$ = new Subject<WsMessage>();
  readonly messages$: Observable<WsMessage> = this._messages$.asObservable();

  private readonly _connectionStatus$ = new Subject<'connected' | 'disconnected' | 'connecting'>();
  readonly connectionStatus$: Observable<'connected' | 'disconnected' | 'connecting'> = this._connectionStatus$.asObservable();

  /**
   * Conecta al WebSocket del servidor.
   */
  connect(): void {
    if (this.ws) {
      this.disconnect();
    }

    this._connectionStatus$.next('connecting');
    this.ws = new WebSocket('ws://127.0.0.1:8000/ws/translate');

    this.ws.onopen = () => {
      this._connectionStatus$.next('connected');
      console.log('[WS] Conectado al servidor de traducción');
    };

    this.ws.onmessage = (event) => {
      try {
        const message: WsMessage = JSON.parse(event.data);
        this._messages$.next(message);
      } catch (err) {
        console.error('[WS] Error al decodificar mensaje del servidor:', err);
      }
    };

    this.ws.onerror = (err) => {
      console.error('[WS] Error en la conexión:', err);
      this._messages$.next({ type: 'error', message: 'Error de conexión con el servidor.' });
    };

    this.ws.onclose = () => {
      this._connectionStatus$.next('disconnected');
      console.log('[WS] Conexión cerrada');
    };
  }

  /**
   * Envía la configuración de idiomas al servidor.
   */
  sendConfig(mode: 'auto' | 'manual', lang1?: string, lang2?: string): void {
    const configMsg: any = { type: 'config', mode };
    if (lang1) configMsg.lang1 = lang1;
    if (lang2) configMsg.lang2 = lang2;
    this.send(configMsg);
  }

  /**
   * Envía un fragmento de audio al servidor para la transcripción en tiempo real.
   */
  sendMeetingChunk(blob: Blob): void {
    const reader = new FileReader();
    reader.readAsDataURL(blob);
    reader.onloadend = () => {
      const base64data = reader.result as string;
      const base64String = base64data.split(',')[1];
      
      this.send({
        type: 'meeting_chunk',
        data: base64String
      });
    };
  }

  /**
   * Avisa al servidor que el usuario dejó de hablar, enviando el audio final.
   */
  sendEndUtterance(blob: Blob): void {
    const reader = new FileReader();
    reader.readAsDataURL(blob);
    reader.onloadend = () => {
      const base64data = reader.result as string;
      const base64String = base64data.split(',')[1];
      
      this.send({
        type: 'end_utterance',
        data: base64String
      });
    };
  }

  /**
   * Envía un comando de reinicio para vaciar los buffers acumulados en el servidor.
   */
  sendReset(): void {
    this.send({ type: 'reset' });
  }

  /**
   * Cierra la conexión activa de WebSocket.
   */
  disconnect(): void {
    if (this.ws) {
      this.ws.close();
      this.ws = null;
    }
    this._connectionStatus$.next('disconnected');
  }

  send(data: any): void {
    if (this.ws && this.ws.readyState === WebSocket.OPEN) {
      this.ws.send(JSON.stringify(data));
    } else {
      console.warn('[WS] Intento de enviar datos con WebSocket cerrado');
    }
  }
}
