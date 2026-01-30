import { createHighlighterCore, type HighlighterCore } from 'shiki/core'
import { createJavaScriptRegexEngine } from 'shiki/engine/javascript'

let highlighterPromise: Promise<HighlighterCore> | null = null

// Fine-grained language imports for lazy loading
// These use default exports, so we import them directly
const langImports = {
  typescript: () => import('shiki/dist/langs/typescript.mjs').then(m => m.default),
  javascript: () => import('shiki/dist/langs/javascript.mjs').then(m => m.default),
  bash: () => import('shiki/dist/langs/bash.mjs').then(m => m.default),
  json: () => import('shiki/dist/langs/json.mjs').then(m => m.default),
  tsx: () => import('shiki/dist/langs/tsx.mjs').then(m => m.default),
  jsx: () => import('shiki/dist/langs/jsx.mjs').then(m => m.default),
  rust: () => import('shiki/dist/langs/rust.mjs').then(m => m.default),
  python: () => import('shiki/dist/langs/python.mjs').then(m => m.default),
}

// Map common language aliases
const langAliases: Record<string, keyof typeof langImports> = {
  ts: 'typescript',
  js: 'javascript',
  sh: 'bash',
  shell: 'bash',
  py: 'python',
  rs: 'rust',
}

// Track which languages have been loaded
const loadedLangs = new Set<string>()

async function getHighlighter(): Promise<HighlighterCore> {
  if (!highlighterPromise) {
    const githubDark = await import('shiki/dist/themes/github-dark.mjs').then(m => m.default)
    highlighterPromise = createHighlighterCore({
      themes: [githubDark],
      langs: [], // Start with no languages, load on demand
      engine: createJavaScriptRegexEngine(),
    })
  }
  return highlighterPromise
}

async function ensureLangLoaded(highlighter: HighlighterCore, lang: string): Promise<string> {
  // Resolve alias
  const resolvedLang = (langAliases[lang] || lang) as keyof typeof langImports
  
  // Check if it's a supported language
  if (!(resolvedLang in langImports)) {
    return 'text' // Fallback to plain text
  }
  
  // Load language if not already loaded
  if (!loadedLangs.has(resolvedLang)) {
    const langModule = await langImports[resolvedLang]()
    await highlighter.loadLanguage(langModule)
    loadedLangs.add(resolvedLang)
  }
  
  return resolvedLang
}

export async function highlightCode(code: string, lang: string): Promise<string> {
  const highlighter = await getHighlighter()
  const finalLang = await ensureLangLoaded(highlighter, lang)
  
  return highlighter.codeToHtml(code, {
    lang: finalLang,
    theme: 'github-dark',
  })
}
