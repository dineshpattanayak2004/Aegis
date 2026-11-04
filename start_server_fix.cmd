@echo off
set AI_PROVIDER=ollama
set GEMINI_API_KEY=your_gemini_key_here
python -m uvicorn aegis_server:app --host 0.0.0.0 --port 8000 --app-dir d:\Aeigis
pause