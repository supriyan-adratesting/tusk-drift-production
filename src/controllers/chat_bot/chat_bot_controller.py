from src import app
from src.controllers.chat_bot import chat_bot_process as chat_bot
from src.controllers.jwt_tokens import jwt_token_required as jwt_token
from flask import request



@app.route('/chat_bot_widget', methods=['POST'])
@jwt_token.chat_bot_token_required
async def chat_bot_widget():
    result = await chat_bot.chatbot_widget(request)
    return result

@app.route('/create_vector_index',endpoint='create_vector_index', methods=['POST'])
async def create_vector_index():
    result = await chat_bot.create_vector(request)
    return result

@app.route('/get_rag_data',endpoint='get_rag_data', methods=['POST'])
async def get_rag_data():
    result = await chat_bot.get_ragData(request)
    return result

@app.route('/upload_document',endpoint='upload_document',methods=['POST'])
async def upload_document():
    result = chat_bot.uploadDocument(request)
    return result