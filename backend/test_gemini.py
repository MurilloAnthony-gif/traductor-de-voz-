import google.generativeai as genai
import os

api_key = "YOUR_API_KEY_HERE"
genai.configure(api_key=api_key)
model = genai.GenerativeModel('gemini-1.5-flash')

try:
    response = model.generate_content("Hello, how are you?")
    print("SUCCESS:", response.text)
except Exception as e:
    print("ERROR:", str(e))
