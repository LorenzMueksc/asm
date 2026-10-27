import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

// npm run dev serves the UI with hot reload and proxies the API to a running asmdev
export default defineConfig({
  plugins: [react()],
  build: { outDir: 'dist', emptyOutDir: true },
  server: { proxy: { '/api': 'http://127.0.0.1:7700' } },
})
