
from src import app
from src.controllers.professional import professional_process as professional
from src.controllers.partner import partner_process as partner
from src.controllers.jwt_tokens import jwt_token_required as jwt_token


@app.route('/professional_signup' ,methods=['GET'])
def partner_signup():
    result = professional.partner_signup() 
    return result

@app.route('/get_partner_team_details',endpoint='get_partner_team_details',methods=['GET'])
@jwt_token.token_required
def get_partner_team_details():
    result = partner.get_partner_team_details()
    return result

@app.route('/update_partner_team_details',endpoint='update_partner_team_details',methods=['POST'])
@jwt_token.token_required
def update_partner_team_details():
    result = partner.update_partner_team_details()
    return result

@app.route('/get_partner_profile_dashboard_data',endpoint='get_partner_profile_dashboard_data',methods=['GET'])
@jwt_token.token_required
def get_partner_profile_dashboard_data():
    result = partner.get_partner_profile_dashboard_data()
    return result

@app.route('/create_post',endpoint='create_post',methods=['POST'])
@jwt_token.token_required
def partner_create_post():
    result = partner.create_post()
    return result

@app.route('/edit_partner_post',endpoint='edit_partner_post',methods=['POST'])
@jwt_token.token_required
def edit_partner_post():
    result = partner.edit_partner_post()
    return result

@app.route('/get_partner_home_view',endpoint='get_partner_home_view',methods=['POST'])
@jwt_token.token_required
def get_partner_home_view():
    result = partner.get_partner_home_view()
    return result

@app.route('/draft_post',endpoint='draft_post',methods=['POST'])
@jwt_token.token_required
def draft_post():
    result = partner.draft_post()
    return result

@app.route('/get_post',endpoint='get_post',methods=['POST'])
@jwt_token.token_required
def get_post():
    result = partner.get_post()
    return result

@app.route('/status_update_posted',endpoint='status_update_posted',methods=['POST'])
@jwt_token.token_required
def status_update_posted():
    result = partner.status_update_posted()
    return result

@app.route('/close_posted',endpoint='close_posted',methods=['POST'])
@jwt_token.token_required
def close_posted():
    result = partner.close_posted()
    return result

@app.route('/delete_learning_post',endpoint='delete_learning_post',methods=['POST'])
@jwt_token.token_required
def delete_learning_post():
    result = partner.delete_learning_post()
    return result

@app.route('/get_ad_post',endpoint='get_ad_post',methods=['POST'])
@jwt_token.token_required
def get_ad_post():
    result = partner.get_ad_posts()
    return result

@app.route('/get_payment_status_partner',endpoint='get_payment_status_partner' ,methods=['GET'])
@jwt_token.token_required
def get_payment_status_employer():
    return partner.get_payment_status_partner()