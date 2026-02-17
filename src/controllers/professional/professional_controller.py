from flask import render_template
from flask import Flask, request, jsonify, session, url_for, redirect

from src import app
from src.controllers.professional import professional_process as professional
from src.controllers.jwt_tokens import jwt_token_required as jwt_token


@app.route('/professional_profile_update',endpoint='professional_profile_update',methods=['POST'])
@jwt_token.token_required
def professional_profile_update():
    result = professional.update_professional_profile()
    return result 

@app.route('/get_professional_profile_data',endpoint='get_professional_profile_data',methods=['GET'])
@jwt_token.token_required
def get_professional_profile_data():
    result = professional.get_professional_profile_data()
    return result

@app.route('/professional_profile_extraction',endpoint='professional_profile_extraction',methods=['POST'])
@jwt_token.token_required
def professional_profile_extraction():
    result = professional.professional_details_update(request)
    return result

@app.route('/professional_index_search',endpoint='professional_index_search',methods=['POST'])
@jwt_token.token_required
def professional_index_search():
    result = professional.professional_meilisearch()
    return result

@app.route('/professional_experience_update',endpoint='professional_experience_update',methods=['POST'])
@jwt_token.token_required
def professional_experience_update():
    result = professional.update_professional_experience()
    return result

@app.route('/get_professional_experience',endpoint='get_professional_experience',methods=['POST'])
@jwt_token.token_required
def get_professional_experience():
    result = professional.get_professional_experience_data()
    return result

@app.route('/professional_experience_delete',endpoint='professional_experience_delete',methods=['POST'])
@jwt_token.token_required
def professional_experience_delete():
    result = professional.delete_professional_experience()
    return result

@app.route('/professional_education_update',endpoint='professional_education_update',methods=['POST'])
@jwt_token.token_required
def professional_education_update():
    result = professional.update_professional_education()
    return result

@app.route('/get_professional_education',endpoint='get_professional_education',methods=['POST'])
@jwt_token.token_required
def get_professional_education():
    result = professional.get_professional_education_data()
    return result

@app.route('/professional_education_delete',endpoint='professional_education_delete',methods=['POST'])
@jwt_token.token_required
def professional_education_delete():
    result = professional.delete_professional_education()
    return result

@app.route('/professional_skills_update',endpoint='professional_skills_update',methods=['POST'])
@jwt_token.token_required
def professional_skills_update():
    result = professional.update_professional_skills()
    return result

@app.route('/get_professional_skills',endpoint='get_professional_skills',methods=['POST'])
@jwt_token.token_required
def get_professional_skills():
    result = professional.get_professional_skills_data()
    return result

@app.route('/get_selected_professional_skill',endpoint='get_selected_professional_skill',methods=['POST'])
@jwt_token.token_required
def get_selected_professional_skill():
    result = professional.get_selected_professional_skill_data()
    return result

@app.route('/professional_skill_delete',endpoint='professional_skill_delete',methods=['POST'])
@jwt_token.token_required
def professional_skill_delete():
    result = professional.delete_professional_skill()
    return result

@app.route('/get_other_employer_skills',endpoint='get_other_employer_skills',methods=['GET'])
@jwt_token.token_required
def get_other_employer_skills():
    result = professional.get_other_employer_skills()
    return result

@app.route('/get_other_employer_specialisation',endpoint='get_other_employer_specialisation',methods=['GET'])
@jwt_token.token_required
def get_other_employer_specialisation():
    result = professional.get_other_employer_specialisation()
    return result

@app.route('/professional_language_update',endpoint='professional_language_update',methods=['POST'])
@jwt_token.token_required
def professional_language_update():
    result = professional.update_professional_language()
    return result

@app.route('/get_professional_language',endpoint='get_professional_language',methods=['POST'])
@jwt_token.token_required
def get_professional_language():
    result = professional.get_professional_language_data()
    return result

@app.route('/professional_language_delete',endpoint='professional_language_delete',methods=['POST'])
@jwt_token.token_required
def professional_language_delete():
    result = professional.delete_professional_language()
    return result

@app.route('/professional_preferences_update',endpoint='professional_preferences_update',methods=['POST'])
@jwt_token.token_required
def professional_preferences_update():
    result = professional.update_professional_preferences()
    return result

@app.route('/get_professional_preferences',endpoint='get_professional_preferences',methods=['POST'])
@jwt_token.token_required
def get_professional_preferences():
    result = professional.get_professional_preferences_data()
    return result

@app.route('/professional_about_update',endpoint='professional_about_update',methods=['POST'])
@jwt_token.token_required
def professional_about_update():
    result = professional.update_professional_about()
    return result

@app.route('/get_professional_about',endpoint='get_professional_about',methods=['POST'])
@jwt_token.token_required
def get_professional_about():
    result = professional.get_professional_about_data()
    return result

@app.route('/professional_socail_link_update',endpoint='professional_socail_link_update',methods=['POST'])
@jwt_token.token_required
def professional_socail_link_update():
    result = professional.update_professional_social_link()
    return result

@app.route('/get_professional_social_link',endpoint='get_professional_social_link',methods=['POST'])
@jwt_token.token_required
def get_professional_social_link():
    result = professional.get_professional_social_link_data()
    return result

@app.route('/get_selected_professional_social_link',endpoint='get_selected_professional_social_link',methods=['POST'])
@jwt_token.token_required
def get_selected_professional_social_link():
    result = professional.get_selected_professional_social_link_data()
    return result

@app.route('/professional_social_link_delete',endpoint='professional_social_link_delete',methods=['POST'])
@jwt_token.token_required
def professional_social_link_delete():
    result = professional.delete_professional_social_link()
    return result

@app.route('/professional_job_apply',endpoint='professional_job_apply' ,methods=['POST'])
@jwt_token.token_required
def professional_job_apply():
    result = professional.professional_job_apply()
    return result
@app.route('/professional_recommended',endpoint='professional_recommended' ,methods=['GET'])
def professional_recommended(): 
    return professional.professional_home_recommended_view()

@app.route('/professional_job_save',endpoint='professional_job_save' ,methods=['POST'])
@jwt_token.token_required
def professional_job_save():
    result = professional.professional_job_save()
    return result

@app.route('/fetch_filter_results',endpoint='fetch_filter_results' ,methods=['POST'])
def fetch_filter_results():
    return professional.fetch_filter_results()

@app.route('/professional_job_share_link',endpoint='professional_job_share_link' ,methods=['POST'])
@jwt_token.token_required
def professional_job_share_link():
    result = professional.get_professional_job_link()
    return result

@app.route('/shared_job_details',endpoint='shared_job_details' ,methods=['GET'])
def shared_job_details():
    return professional.get_shared_job_details()

@app.route('/get_attachment',endpoint='get_attachment' ,methods=['GET'])
def shared_job_details():
    return professional.get_learning_attachment()

# @app.route('/update_dob_status',endpoint='update_dob_status' ,methods=['POST'])
# def update_dob_status():
#     return professional.update_dob_status()
    
@app.route('/show_pages',endpoint='show_pages' ,methods=['POST'])
def show_pages():
    return professional.show_pages()

@app.route('/professional_profile_dashboard',endpoint='professional_profile_dashboard' ,methods=['POST'])
@jwt_token.token_required
def professional_profile_dashboard():
    result = professional.get_professional_profile_dashboard()
    return result

@app.route('/generate_cover_letter',endpoint='generate_cover_letter' ,methods=['POST'])
@jwt_token.token_required
def cover_letter():
    result = professional.cover_letter()
    return result

@app.route('/professional_dashboard',endpoint='professional_dashboard' ,methods=['POST'])
@jwt_token.token_required
def professional_dashboard():
    return professional.get_professional_dashboard_data()

@app.route('/user_dashboard_details',endpoint='user_dashboard_details' ,methods=['GET'])
@jwt_token.token_required
def user_dashboard_details():
    return professional.user_dashboard_details()

@app.route('/professional_applied_jobs',endpoint='professional_applied_jobs' ,methods=['POST'])
def professional_applied_jobs(): 
    return professional.professional_home_applied_view()

@app.route('/professional_saved_jobs',endpoint='professional_saved_jobs' ,methods=['POST'])
@jwt_token.token_required
def professional_saved_jobs():
    return professional.get_professional_saved_jobs()

@app.route('/selected_job_details',endpoint='selected_job_details' ,methods=['POST'])
def selected_job_details():  
    return professional.selected_job_details()

@app.route('/professional_dashboard_filter_list',endpoint='professional_dashboard_filter_list' ,methods=['POST'])
@jwt_token.token_required
def professional_dashboard_filter_list():
    return professional.get_professional_dashboard_filter_list()

@app.route('/professional_notifications',endpoint='professional_notifications' ,methods=['GET'])
@jwt_token.token_required
def professional_notifications():
    return professional.get_professional_notifications()

@app.route('/learning_page_get_all',endpoint='learning_page_get_all' ,methods=['GET'])
@jwt_token.token_required
def learning_page_get_all():
    return professional.learning_page_get_all()

@app.route('/professional_community',endpoint='professional_community' ,methods=['POST'])
@jwt_token.token_required
def professional_community():
    return professional.professional_community()

@app.route('/professional_learning',endpoint='professional_learning' ,methods=['POST'])
@jwt_token.token_required
def professional_learning():
    return professional.professional_learning()

@app.route('/professional_events',endpoint='professional_events' ,methods=['GET'])
@jwt_token.token_required
def professional_events():
    return professional.professional_events()

@app.route('/professional_get_perspectives',endpoint='professional_get_perspectives' ,methods=['GET'])
@jwt_token.token_required
def professional_get_perspectives():
    return professional.professional_get_perspectives()

@app.route('/learning_page_search_filter',endpoint='learning_page_search_filter' ,methods=['POST'])
@jwt_token.token_required
def learning_page_search_filter():
    return professional.learning_page_search_filter()

@app.route('/professional_discourse_community',endpoint='professional_discourse_community' ,methods=['GET'])
@jwt_token.token_required
def professional_discourse_community():
    return professional.professional_discourse_community()

@app.route('/get_additional_info',endpoint='get_additional_info' ,methods=['POST'])
@jwt_token.token_required
def get_additional_info():
    return professional.get_additional_info()

@app.route('/update_professional_additional_info',endpoint='update_professional_additional_info' ,methods=['POST'])
@jwt_token.token_required
def update_professional_additional_info():
    return professional.update_professional_additional_info()

@app.route('/delete_professional_additional_info',endpoint='delete_professional_additional_info' ,methods=['POST'])
@jwt_token.token_required
def delete_professional_additional_info():
    return professional.delete_professional_additional_info()

@app.route('/update_expert_notes',endpoint='update_expert_notes' ,methods=['POST'])
@jwt_token.token_required
def update_expert_notes():
    return professional.update_expert_notes()

@app.route('/pre_screen_job_questions',endpoint='pre_screen_job_questions' ,methods=['POST'])
@jwt_token.token_required
def pre_screen_job_questions():
    return professional.professional_job_questions()

@app.route('/professional_video_upload',endpoint='professional_video_upload' ,methods=['POST'])
@jwt_token.token_required
def professional_video_upload():
    return professional.professional_intro_video_upload()    


@app.route('/upload_user_profile_pic',endpoint='upload_user_profile_pic' ,methods=['POST'])
@jwt_token.token_required
def upload_user_profile_pic():
    return professional.upload_user_profile_pic()

@app.route('/delete_user_profile_pic',endpoint='delete_user_profile_pic' ,methods=['POST'])
@jwt_token.token_required
def delete_user_profile_pic():
    return professional.delete_user_profile_pic()

@app.route('/delete_user_resume',endpoint='delete_user_resume' ,methods=['GET'])
@jwt_token.token_required
def delete_user_resume():
    return professional.delete_user_resume()

@app.route('/check_user_resume',endpoint='check_user_resume' ,methods=['GET'])
@jwt_token.token_required
def check_user_resume():
    return professional.check_user_resume()

@app.route('/delete_professional_intro_video',endpoint='delete_professional_intro_video' ,methods=['POST'])
@jwt_token.token_required
def delete_professional_intro_video():
    return professional.delete_profile_intro_video()

@app.route('/store_signup_details',endpoint='store_signup_details' ,methods=['POST'])
def store_signup_details():
    return professional.store_signup_details()

@app.route('/professional_onClick_apply_job',endpoint='professional_onClick_apply_job' ,methods=['POST'])
@jwt_token.token_required
def professional_onClick_apply_job():
    return professional.professional_onClick_apply_job()

@app.route('/get_detailed_description_learning',endpoint='get_detailed_description_learning' ,methods=['POST'])
@jwt_token.token_required
def get_detailed_description_learning():
    return professional.get_detailed_description_learning()
    
@app.route('/get_detailed_description_community',endpoint='get_detailed_description_community' ,methods=['POST'])
@jwt_token.token_required
def get_detailed_description_community():
    return professional.get_detailed_description_community()

@app.route('/unsave_job',endpoint='unsave_job' ,methods=['POST'])
@jwt_token.token_required
def unsave_job():
    return professional.unsave_job()

@app.route('/notification_status',endpoint='notification_status' ,methods=['POST'])
@jwt_token.token_required
def notification_status():
    return professional.notification_status()

@app.route('/delete_notifications',endpoint='delete_notifications' ,methods=['POST'])
@jwt_token.token_required
def delete_notifications():
    return professional.delete_notifications()

@app.route('/clear_all_notifications',endpoint='clear_all_notifications' ,methods=['GET'])
@jwt_token.token_required
def clear_all_notifications():
    return professional.clear_all_notifications()

@app.route('/update_partner_view_count',endpoint='update_partner_view_count',methods=['POST'])
@jwt_token.token_required
def update_partner_view_count():
    result = professional.update_partner_view_count()
    return result

@app.route('/update_expert_notes_status',endpoint='update_expert_notes_status' ,methods=['POST'])
@jwt_token.token_required
def update_expert_notes_status():
    return professional.update_expert_notes_status()

@app.route('/professional_updated_home',endpoint='professional_updated_home' ,methods=['GET'])
@jwt_token.token_required
def professional_updated_home():
    return professional.professional_updated_home()

@app.route('/update_home_page_video',endpoint='update_home_page_video' ,methods=['POST'])
@jwt_token.token_required
def update_home_page_video():
    return professional.update_home_page_video()

@app.route('/api/ip', methods=['GET'])
def get1_ip():
    return professional.get1_ip()

@app.route('/search_result',endpoint='search_result' ,methods=['POST'])
@jwt_token.token_required
def search_result():
    return professional.search_result()

@app.route('/search_result_applied',endpoint='search_result_applied' ,methods=['POST'])
@jwt_token.token_required
def search_result_applied():
    return professional.search_result_applied()

@app.route('/search_result_saved',endpoint='search_result_saved' ,methods=['POST'])
@jwt_token.token_required
def search_result_saved():
    return professional.search_result_saved()

@app.route('/get_payment_status_professional',endpoint='get_payment_status_professional' ,methods=['GET'])
@jwt_token.token_required
def get_payment_status_professional():
    return professional.get_payment_status_professional()

@app.route('/get_help_videos_details',endpoint='get_help_videos_details' ,methods=['GET'])
@jwt_token.token_required
def get_help_videos_details():
    return professional.get_help_videos_details()

@app.route('/get_training_data',endpoint='get_training_data' ,methods=['GET'])
@jwt_token.token_required
def get_training_data():
    return professional.get_training_data()

# @app.route('/mixpanel_professional_weekly_signup',endpoint='mixpanel_professional_weekly_signup' ,methods=['GET'])
# @jwt_token.token_required
# def mixpanel_professional_weekly_signup():
#     return professional.mixpanel_professional_weekly_signup()

# @app.route('/get_professional_id',endpoint='get_professional_id' ,methods=['GET'])
# def get_professional_id():
#     return professional.get_professional_id()

# @app.route('/track_user_event',endpoint='track_user_event' ,methods=['GET'])
# def track_user_event():
#     return professional.track_user_event()

# @app.route('/track_partner_post',endpoint='track_partner_post' ,methods=['GET'])
# def track_partner_post():
#     return professional.track_partner_post()

# @app.route('/track_job_post',endpoint='track_job_post' ,methods=['GET'])
# def track_job_post():
#     return professional.track_job_post()

# @app.route('/track_prof_users',endpoint='track_prof_users' ,methods=['GET'])
# def track_prof_users():
#     return professional.track_prof_users()

# @app.route('/get_professional_profile_detail',endpoint='get_professional_profile_detail' ,methods=['GET'])
# def get_professional_profile_detail():
#     return professional.get_professional_profile_detail()