import google.generativeai as genai
import os

api_key = "YOUR_API_KEY_HERE"
genai.configure(api_key=api_key)

try:
    for m in genai.list_models():
        if 'generateContent' in m.supported_generation_methods:
            print(m.name)
except Exception as e:
    print("ERROR:", str(e))
