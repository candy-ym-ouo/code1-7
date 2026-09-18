import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { createRoot } from 'react-dom/client';
import './style.css';

const API = (import.meta.env.VITE_API_URL || 'http://localhost:4000').replace(/\/$/, '');

type User = {
  id: string;
  email: string;
  displayName: string;
};

type AuthResponse = {
  token: string;
  user: User;
};

type WorkspaceModel = {
  id: string;
  name: string;
  timezone: string;
};

type Recording = {
  id: string;
  title: string;
  sizeBytes: string | number;
  durationMs: number;
  status: 'UPLOADING' | 'PROCESSING' | 'READY' | 'FAILED';
  processingError?: string | null;
  _count?: { clips: number };
};

type Clip = {
  id: string;
  title: string;
  startMs: number;
  endMs: number;
  summary: string;
  version: number;
};

type ApiErrorPayload = {
  error?: {
    code?: string;
    message?: string;
  };
};

class ApiError extends Error {
  constructor(
    message: string,
    public readonly status: number,
  ) {
    super(message);
    this.name = 'ApiError';
  }
}

async function api<T>(path: string, init?: RequestInit): Promise<T> {
  const headers = new Headers(init?.headers);
  const token = localStorage.getItem('token');
  const isFormData = init?.body instanceof FormData;

  if (token) headers.set('Authorization', `Bearer ${token}`);
  if (init?.body && !isFormData && !headers.has('Content-Type')) {
    headers.set('Content-Type', 'application/json');
  }

  const response = await fetch(`${API}${path}`, { ...init, headers });
  const payload = (await response.json().catch(() => ({}))) as ApiErrorPayload & {
    data?: T;
  };

  if (!response.ok) {
    if (response.status === 401 && !path.startsWith('/v1/auth/')) {
      window.dispatchEvent(new Event('history:auth-expired'));
    }
    throw new ApiError(payload.error?.message || '请求失败，请稍后重试', response.status);
  }

  if (payload.data === undefined) {
    throw new ApiError('服务器返回格式错误', response.status);
  }
  return payload.data;
}

function Login({ onLogin }: { onLogin: (token: string) => void }) {
  const [email, setEmail] = useState('');
  const [password, setPassword] = useState('');
  const [register, setRegister] = useState(false);
  const [error, setError] = useState('');
  const [busy, setBusy] = useState(false);

  const submit = async (event: React.FormEvent) => {
    event.preventDefault();
    setError('');
    setBusy(true);
    try {
      const result = await api<AuthResponse>(
        `/v1/auth/${register ? 'register' : 'login'}`,
        {
          method: 'POST',
          body: JSON.stringify({
            email: email.trim(),
            password,
            displayName: email.split('@')[0],
          }),
        },
      );
      localStorage.setItem('token', result.token);
      onLogin(result.token);
    } catch (submitError) {
      setError((submitError as Error).message);
    } finally {
      setBusy(false);
    }
  };

  return (
    <main className="auth">
      <form onSubmit={submit}>
        <div className="mark">家史</div>
        <h1>口述家史编辑器</h1>
        <p className="muted">把访谈录音整理成可阅读的家庭章节</p>
        <label>
          邮箱
          <input
            value={email}
            onChange={(event) => setEmail(event.target.value)}
            type="email"
            autoComplete="email"
            required
          />
        </label>
        <label>
          密码
          <input
            value={password}
            onChange={(event) => setPassword(event.target.value)}
            type="password"
            autoComplete={register ? 'new-password' : 'current-password'}
            minLength={8}
            required
          />
        </label>
        {error && <div className="error">{error}</div>}
        <button disabled={busy}>{busy ? '处理中...' : register ? '创建账户' : '登录'}</button>
        <button
          type="button"
          className="ghost"
          disabled={busy}
          onClick={() => {
            setError('');
            setRegister((value) => !value);
          }}
        >
          {register ? '已有账户，去登录' : '首次使用，创建账户'}
        </button>
      </form>
    </main>
  );
}

function App() {
  const [token, setToken] = useState(localStorage.getItem('token'));

  useEffect(() => {
    const logout = () => {
      localStorage.removeItem('token');
      setToken(null);
    };
    window.addEventListener('history:auth-expired', logout);
    return () => window.removeEventListener('history:auth-expired', logout);
  }, []);

  if (!token) return <Login onLogin={setToken} />;

  return (
    <Workspace
      onLogout={() => {
        localStorage.removeItem('token');
        setToken(null);
      }}
    />
  );
}

function Workspace({ onLogout }: { onLogout: () => void }) {
  const [workspace, setWorkspace] = useState<WorkspaceModel | null>(null);
  const [recordings, setRecordings] = useState<Recording[]>([]);
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const [clips, setClips] = useState<Clip[]>([]);
  const [busy, setBusy] = useState(false);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');
  const fileInput = useRef<HTMLInputElement>(null);

  const selected = useMemo(
    () => recordings.find((recording) => recording.id === selectedId) || null,
    [recordings, selectedId],
  );

  const loadRecordings = useCallback(async () => {
    if (!workspace) return;
    const rows = await api<Recording[]>(`/v1/workspaces/${workspace.id}/recordings`);
    setRecordings(rows);
  }, [workspace]);

  useEffect(() => {
    let cancelled = false;

    const bootstrap = async () => {
      setLoading(true);
      setError('');
      try {
        let workspaces = await api<WorkspaceModel[]>('/v1/workspaces');
        if (workspaces.length === 0) {
          const created = await api<WorkspaceModel>('/v1/workspaces', {
            method: 'POST',
            body: JSON.stringify({ name: '我的家史' }),
          });
          workspaces = [created];
        }

        const current = workspaces[0];
        const rows = await api<Recording[]>(
          `/v1/workspaces/${current.id}/recordings`,
        );
        if (cancelled) return;
        setWorkspace(current);
        setRecordings(rows);
      } catch (loadError) {
        if (!cancelled) setError((loadError as Error).message);
      } finally {
        if (!cancelled) setLoading(false);
      }
    };

    void bootstrap();
    return () => {
      cancelled = true;
    };
  }, []);

  const hasActiveProcessing = recordings.some(
    (recording) =>
      recording.status === 'UPLOADING' || recording.status === 'PROCESSING',
  );

  useEffect(() => {
    if (!workspace || !hasActiveProcessing) return;
    const timer = window.setInterval(() => {
      void loadRecordings().catch((pollError) => {
        setError((pollError as Error).message);
      });
    }, 2000);
    return () => window.clearInterval(timer);
  }, [workspace, hasActiveProcessing, loadRecordings]);

  useEffect(() => {
    if (!selected) {
      setClips([]);
      return;
    }

    let cancelled = false;
    if (selected.status !== 'READY') {
      setClips([]);
      return;
    }

    void api<Clip[]>(`/v1/recordings/${selected.id}/clips`)
      .then((rows) => {
        if (!cancelled) setClips(rows);
      })
      .catch((clipError) => {
        if (!cancelled) setError((clipError as Error).message);
      });

    return () => {
      cancelled = true;
    };
  }, [selected]);

  const upload = async (event: React.ChangeEvent<HTMLInputElement>) => {
    const file = event.target.files?.[0];
    if (!file || !workspace) return;

    setBusy(true);
    setError('');
    const form = new FormData();
    form.append('file', file);
    // 同一次上传的任何重试（超时、网络抖动后重发）复用幂等键，服务端不会建第二份录音
    const idempotencyKey =
      typeof crypto.randomUUID === 'function'
        ? crypto.randomUUID()
        : `${Date.now()}-${Math.random().toString(36).slice(2)}`;

    try {
      await api<Recording>(`/v1/workspaces/${workspace.id}/recordings/uploads`, {
        method: 'POST',
        body: form,
        headers: { 'Idempotency-Key': idempotencyKey },
      });
      await loadRecordings();
    } catch (uploadError) {
      setError((uploadError as Error).message);
    } finally {
      setBusy(false);
      if (fileInput.current) fileInput.current.value = '';
    }
  };

  return (
    <div className="shell">
      <header>
        <div className="brand">
          <span className="mark small">家史</span>
          <strong>{workspace?.name || '口述家史'}</strong>
        </div>
        <nav>
          <button className="ghost" onClick={onLogout}>
            退出
          </button>
        </nav>
      </header>

      <div className="layout">
        <aside>
          <div className="aside-head">
            <span>访谈录音</span>
            <label className={`upload ${busy ? 'disabled' : ''}`}>
              + 上传录音
              <input
                ref={fileInput}
                type="file"
                accept="audio/*,.m4a,.flac,.aac,.ogg,.opus"
                onChange={upload}
                disabled={busy || !workspace}
              />
            </label>
          </div>
          {busy && <div className="progress">正在上传，请勿关闭页面...</div>}
          {error && <div className="error sidebar-error">{error}</div>}
          {loading && <p className="empty">正在加载工作区...</p>}
          {!loading &&
            recordings.map((recording) => (
              <button
                key={recording.id}
                type="button"
                className={`recording ${selectedId === recording.id ? 'active' : ''}`}
                onClick={() => setSelectedId(recording.id)}
              >
                <span className="play">▶</span>
                <span>
                  <b>{recording.title}</b>
                  <small>
                    {recording.status === 'READY'
                      ? '可编辑'
                      : recording.status === 'FAILED'
                        ? '处理失败'
                        : '处理中'}{' '}
                    · {recording._count?.clips || 0} 个片段
                  </small>
                </span>
              </button>
            ))}
          {!loading && !recordings.length && !busy && (
            <p className="empty">上传一段访谈录音开始整理。</p>
          )}
        </aside>

        <section className="content">
          {selected ? (
            <Editor
              key={selected.id}
              recording={selected}
              clips={clips}
              setClips={setClips}
            />
          ) : (
            <div className="welcome">
              <div className="wave decorative">〰 〰 〰</div>
              <h2>从一段声音开始</h2>
              <p>选择左侧录音，在时间轴上标记片段并整理内容。</p>
            </div>
          )}
        </section>
      </div>
    </div>
  );
}

function formatTime(milliseconds: number) {
  const safeMs = Number.isFinite(milliseconds) ? Math.max(0, milliseconds) : 0;
  const totalSeconds = Math.floor(safeMs / 1000);
  const hours = Math.floor(totalSeconds / 3600);
  const minutes = Math.floor((totalSeconds % 3600) / 60);
  const seconds = totalSeconds % 60;
  const millis = Math.floor(safeMs % 1000);
  const prefix = hours > 0 ? `${String(hours).padStart(2, '0')}:` : '';
  return `${prefix}${String(minutes).padStart(2, '0')}:${String(seconds).padStart(2, '0')}.${String(millis).padStart(3, '0')}`;
}

function Editor({
  recording,
  clips,
  setClips,
}: {
  recording: Recording;
  clips: Clip[];
  setClips: React.Dispatch<React.SetStateAction<Clip[]>>;
}) {
  const audio = useRef<HTMLAudioElement>(null);
  const playbackEnd = useRef<number | null>(null);
  const [duration, setDuration] = useState(recording.durationMs || 0);
  const [draft, setDraft] = useState({
    title: '新片段',
    startMs: 0,
    endMs: Math.min(recording.durationMs || 10_000, 10_000),
    summary: '',
  });
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState('');
  const token = localStorage.getItem('token') || '';
  const audioUrl = `${API}/v1/recordings/${recording.id}/file?token=${encodeURIComponent(token)}`;
  const timelineDuration = duration > 0 ? duration : recording.durationMs;

  useEffect(() => {
    setDuration(recording.durationMs || 0);
    setDraft({
      title: '新片段',
      startMs: 0,
      endMs: Math.min(recording.durationMs || 10_000, 10_000),
      summary: '',
    });
    setError('');
  }, [recording.id, recording.durationMs]);

  const addClip = async () => {
    const title = draft.title.trim();
    if (!title) {
      setError('请输入片段标题');
      return;
    }
    if (
      !Number.isInteger(draft.startMs) ||
      !Number.isInteger(draft.endMs) ||
      draft.startMs < 0 ||
      draft.endMs <= draft.startMs
    ) {
      setError('出点必须大于入点');
      return;
    }
    if (timelineDuration > 0 && draft.endMs > timelineDuration) {
      setError('出点不能超过录音时长');
      return;
    }

    setSaving(true);
    setError('');
    try {
      const created = await api<Clip>(`/v1/recordings/${recording.id}/clips`, {
        method: 'POST',
        body: JSON.stringify({
          ...draft,
          title,
          transcript: '',
        }),
      });
      setClips((current) =>
        [...current, created].sort((a, b) => a.startMs - b.startMs),
      );
      setDraft((current) => ({
        title: '新片段',
        startMs: current.endMs,
        endMs: Math.min(current.endMs + 10_000, timelineDuration),
        summary: '',
      }));
    } catch (saveError) {
      setError((saveError as Error).message);
    } finally {
      setSaving(false);
    }
  };

  const playClip = (clip: Clip) => {
    const element = audio.current;
    if (!element) return;
    playbackEnd.current = clip.endMs / 1000;
    element.currentTime = clip.startMs / 1000;
    void element.play().catch((playError) => {
      setError((playError as Error).message);
    });
  };

  if (recording.status !== 'READY') {
    return (
      <div className="editor">
        <div className="editor-head">
          <div>
            <span className="eyebrow">录音</span>
            <h2>{recording.title}</h2>
          </div>
          <span className={`status ${recording.status.toLowerCase()}`}>
            {recording.status}
          </span>
        </div>
        <div className="processing">
          {recording.status === 'FAILED'
            ? `处理失败：${recording.processingError || '请稍后重试'}`
            : '录音正在处理中，完成后即可创建片段。'}
        </div>
      </div>
    );
  }

  return (
    <div className="editor">
      <div className="editor-head">
        <div>
          <span className="eyebrow">录音</span>
          <h2>{recording.title}</h2>
        </div>
        <span className="status">READY</span>
      </div>

      <audio
        ref={audio}
        controls
        src={audioUrl}
        onLoadedMetadata={(event) => {
          const nextDuration = event.currentTarget.duration * 1000;
          if (Number.isFinite(nextDuration) && nextDuration > 0) {
            setDuration(nextDuration);
          }
        }}
        onTimeUpdate={(event) => {
          const end = playbackEnd.current;
          if (end !== null && event.currentTarget.currentTime >= end) {
            event.currentTarget.pause();
            playbackEnd.current = null;
          }
        }}
        onEnded={() => {
          playbackEnd.current = null;
        }}
      />

      <div className="timeline">
        <div className="ruler">
          <span>00:00.000</span>
          <span>{formatTime(timelineDuration / 2)}</span>
          <span>{formatTime(timelineDuration)}</span>
        </div>
        <div className="waveform">
          {Array.from({ length: 80 }, (_, index) => (
            <i
              key={index}
              style={{ height: `${18 + Math.abs(Math.sin(index * 1.7)) * 60}%` }}
            />
          ))}
          {timelineDuration > 0 &&
            clips.map((clip) => (
              <div
                className="clip"
                key={clip.id}
                style={{
                  left: `${Math.max(0, Math.min(100, (clip.startMs / timelineDuration) * 100))}%`,
                  width: `${Math.max(
                    1,
                    Math.min(
                      100,
                      ((clip.endMs - clip.startMs) / timelineDuration) * 100,
                    ),
                  )}%`,
                }}
                title={clip.title}
              >
                {clip.title}
              </div>
            ))}
        </div>
      </div>

      <div className="clip-form">
        <div className="form-title">新建片段</div>
        <label className="title-field">
          标题
          <input
            value={draft.title}
            onChange={(event) =>
              setDraft((current) => ({ ...current, title: event.target.value }))
            }
            placeholder="片段标题"
          />
        </label>
        <label>
          入点 (毫秒)
          <input
            type="number"
            min="0"
            step="1"
            value={draft.startMs}
            onChange={(event) =>
              setDraft((current) => ({
                ...current,
                startMs: Number(event.target.value),
              }))
            }
          />
        </label>
        <label>
          出点 (毫秒)
          <input
            type="number"
            min="1"
            step="1"
            value={draft.endMs}
            onChange={(event) =>
              setDraft((current) => ({
                ...current,
                endMs: Number(event.target.value),
              }))
            }
          />
        </label>
        <label className="summary-field">
          摘要
          <input
            value={draft.summary}
            onChange={(event) =>
              setDraft((current) => ({ ...current, summary: event.target.value }))
            }
            placeholder="摘要（可选）"
          />
        </label>
        <button type="button" onClick={addClip} disabled={saving}>
          {saving ? '保存中...' : '保存片段'}
        </button>
      </div>

      {error && <div className="error editor-error">{error}</div>}

      <div className="clips">
        <div className="section-title">
          片段列表 <span>{clips.length}</span>
        </div>
        {clips.map((clip) => (
          <div className="clip-row" key={clip.id}>
            <button
              type="button"
              className="icon"
              aria-label={`播放 ${clip.title}`}
              onClick={() => playClip(clip)}
            >
              ▶
            </button>
            <div>
              <b>{clip.title}</b>
              <small>
                {formatTime(clip.startMs)} - {formatTime(clip.endMs)} ·{' '}
                {clip.summary || '暂无摘要'}
              </small>
            </div>
          </div>
        ))}
        {!clips.length && <p className="empty clip-empty">还没有片段。</p>}
      </div>
    </div>
  );
}

const root = document.getElementById('root');
if (!root) throw new Error('缺少 #root 挂载节点');
createRoot(root).render(<App />);
