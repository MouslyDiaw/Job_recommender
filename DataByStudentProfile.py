import pandas as pd

import logging
logger = logging.getLogger('job_recommender')
hdlr = logging.FileHandler('job_recommender.log')
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
hdlr.setFormatter(formatter)
logger.addHandler(hdlr) 
logger.setLevel(logging.INFO)

class LoadDataByStudent():
    """ Import Data for each school_id  """    
        
    def __init__(self, cursor, student_id):
        self.cursor=cursor
        self.student_id=student_id
        
    def TakeData(self):
        logger.info('Start import of students for school %s' % self.student_id)
        self.cursor.execute(self.sql())
        logger.info('Succeeded import of students for school %s' % self.student_id)
        return pd.DataFrame(self.cursor.fetchall(),
                    columns=[desc[0] for desc in self.cursor.description])
    
    def sql(self):
        return """   
                SELECT DISTINCT sp.id,ssp.school_id,job_search_type,
                                us.content_locales langue_search,us.locale,us.email
                           FROM student_profiles sp 
                      LEFT JOIN schools_student_profiles ssp
                             ON sp.id = ssp.student_profile_id
                      LEFT JOIN users us
                             ON sp.user_id=us.id 
                          WHERE sp.daily_job_offers_alert=1 AND 
                                us.current_sign_in_at >= DATEADD(month,-3, GETDATE()) AND
                                sp.id=%s
                """% self.student_id
