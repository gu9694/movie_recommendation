from flask import Flask, session, request, g
from flask import url_for, redirect, render_template
from flask_login import login_user, login_required
from flask_login import LoginManager, current_user
from flask_login import logout_user,UserMixin
from pyspark import  SparkContext
from pyspark.mllib.recommendation import Rating
from pyspark.mllib.recommendation import ALS
import numpy as np
import pandas_gbq
import time
from google.oauth2 import service_account
import config

import os
app = Flask(__name__)
# app.config['SECRET_KEY']=os.urandom(24)
app.config.from_object(config)

login_manager = LoginManager()
login_manager.session_protection = 'strong'
login_manager.login_view = 'login'
login_manager.init_app(app=app)



@login_manager.user_loader
def load_user(user_id):
  user=UserMixin()
  user.id=user_id
  return user

# @app.route('/')
# def index():
#     return render_template('index_2.html')
# #before request:在请求之前执行的，是在视图函数执行之前执行的
# #只是一个装饰器，他可以把需要设置为钩子函数的代码放到视图函数执行之前来执行
# #在每一个视图函数执行之前，都会先执行这个
@app.before_request
def my_before_request():
    g.credentials = service_account.Credentials.from_service_account_file('homework0-b1ec1bd9b9fd.json')
    g.project_id = "homework0-253119"
    pandas_gbq.context.credentials = g.credentials
    pandas_gbq.context.project = g.project_id
    if session.get('username'):
         g.username = session.get('username')
    print('hello world')


# #在每一个视图函数执行时，都会有一个字典作为关键字和值传入
@app.context_processor
def my_context_processor():
    if session.get('user_email'):
        username = session.get('user_email')
        user_id = session.get('uer_id')
        print(request.path)
        return {'user_email':username,'user_id':user_id,'path':request.path}
    print(request.path)
    return {'path':request.path}
    


@app.route('/',methods=['GET','POST'])
def index():
    #获取参数
    #request.args
    if request.method=='GET':
        return render_template('index.html')
        #print(request.form.get('value1'))
    else:
        pass
        #print(request.form.get('value1'))
        # return render_template('login.html')
        
@app.route('/login/',methods=['GET','POST'])
def login():
    if request.method == 'GET':
        return render_template('login.html')
    else:
        email=request.form.get('email')
        password=request.form.get('password')
        print(email,password)
        SQL = """
            SELECT password
            FROM `movie.users`
            WHERE email='%s'
            """%(email)
        try:
            df = pandas_gbq.read_gbq(SQL)
            print('success')
            print(df)
            if len(df)>0:
                if df.iloc[0].password==password:
                    #print('iam here')
                    cur_user = UserMixin()
                    cur_user.id=email
                    login_user(cur_user)
                    session['user_email'] = email
                    SQL = """
                            SELECT uid
                            FROM `movie.users`
                            WHERE email='%s'
                            """%(email)
                    df = pandas_gbq.read_gbq(SQL)
                    session['user_id'] = int(df.iloc[0].uid)
                    session.permenant = True
                    return redirect(url_for('index'))
                else:
                    error = 'email or password is wrong, try again'
                    return render_template('login.html',error=error)
            else:
                error = 'user does not exist'
                return render_template('login.html',error=error)
        except:
            error = 'something wrong try again'
            return render_template('login.html',error=error)
@app.route('/signup/',methods=['GET','POST'])
def signup():
    if request.method == 'GET':
        return render_template('signup.html')
    else:
        email=request.form.get('email')
        first_name=request.form.get('first_name')
        last_name=request.form.get('last_name')
        gender=request.form.get('gender')
        age=request.form.get('age')
        password1=request.form.get('password1')
        password2=request.form.get('password2')
        print(type(email),first_name,last_name,gender,age)
        #查看是否有未填写
        if not (email and first_name and last_name and gender and age and password1 and password2):
            error="Empty fields detected"
            return render_template('signup.html',error_signup=error)
        #验证密码是否相等
        if password1!=password2:
            return '2 passwords not matched'
        #'email'验证，是否被注册过
        SQL1 = """
            SELECT email
            FROM `movie.users`
            WHERE email='%s'
            """%(email)
        SQL2 = """
            SELECT uid
            FROM `movie.users`
            """

        try:
            df1 = pandas_gbq.read_gbq(SQL1,project_id=g.project_id,credentials=g.credentials)
            df2 = pandas_gbq.read_gbq(SQL2,project_id=g.project_id,credentials=g.credentials)
            
            print('success')
            if len(df1)>0:
                error="email already exists, please change an email"
                print(error)
                return render_template('signup.html',error_signup=error)
            else:
                print('heloo')
                uid = df2.uid.unique().max()+1
                print('uid',uid)
                print('i am here')
                SQL3 = """
                INSERT INTO movie.users VALUES (%d,'%s','%s','%s',%d,'%s','%s')
                """%(int(uid),first_name,last_name,gender,int(age),email,password1)
                pandas_gbq.read_gbq(SQL3,project_id=g.project_id,credentials=g.credentials)
                print('success2')
                session['user_id']=uid
                session['user_email']=email
                cur_user = UserMixin()
                cur_user.id=email
                login_user(cur_user)
                #注册成功，跳转到登陆页面
                return redirect(url_for('index'))
        except:
            error="Some fields failed"
            return render_template('signup.html',error_signup=error)
@app.route('/logout')
def logout():
    #session.pop('user_email')
    #session,pop('user_id')
    #del session['user_email]
    session.clear()
    return redirect(url_for('login'))

@app.route('/rate_movies/',methods=['GET','POST'])
@login_required
def rate_movies():
    if request.method=='GET':
        SQL = """
            SELECT *
            FROM `movie.img_url`
            """
        g.df = pandas_gbq.read_gbq(SQL)
        session['id_list']=list(g.df.movie_id)
        return render_template('rate_movies.html',df=g.df)
    else:
        render_template('message.html')
        #id_list=list(g.df.movie_id)
        rate_list=[]
        for i in range(16):
            rate=request.form.get('{}'.format(session['id_list'][i]))
            if rate>='0' and rate<='5':
                rate_list.append(int(rate))
            else:
                rate_list.append(2)
        SQL="""
            DELETE FROM movie.ratings WHERE user_id = {} and item_id in {}
            """.format(session.get('user_id'),tuple(session['id_list']))
        pandas_gbq.read_gbq(SQL)
        for i in range(16):
            SQL2="""
                INSERT INTO movie.ratings VALUES (%d, %d, %d, %d)
                """%(session.get('user_id'), session['id_list'][i],rate_list[i],int(time.time()))
            print(SQL2)
            pandas_gbq.read_gbq(SQL2)
        print(rate_list)
        return render_template('message.html',just_rate=True)

@app.route('/result/')
@login_required
def result():
    SQL = """
            SELECT *
            FROM `movie.ratings`
    """
    SQL2 = """
            SELECT movie_id, movie_title, IMDb_URL
            FROM `movie.movies`
    """

    df_r = pandas_gbq.read_gbq(SQL)
    df_movie = pandas_gbq.read_gbq(SQL2)
    sc = SparkContext.getOrCreate()
    rawUserData=sc.parallelize(np.array(df_r).tolist())
    itemRDD = sc.parallelize(np.array(df_movie).tolist())
    rawRatings = rawUserData.map(lambda line:line[:3]) # 只要前三列,并通过\t分割
    ratingsRDD = rawRatings.map(lambda x:(x[0],x[1],x[2]))
    model = ALS.train(ratingsRDD,10,10,0.01)
    movieTitle = itemRDD.map(lambda a:(float(a[0]),a[1])).collectAsMap() # 创建一个字典,先划分数据变成RDD类型,然后转换成字典,键是id,值是电影名字
    movieUrl = itemRDD.map(lambda a:(float(a[0]),a[2])).collectAsMap()
    recommendP = model.recommendProducts(session.get('user_id'),10) # 返回参数 第二个是产品
    print('For user'+session.get('user_email')+'recommend movie\n')
    for p,rec in enumerate(recommendP):
        print(str(movieTitle[rec.product])+'; recommendation rating '+'{:.3f}'.format(rec.rating)+'\n')
    return render_template('result.html',movieTitle=movieTitle,recommendP=recommendP,movieUrl=movieUrl)
if __name__=='__main__':
    app.run(debug=True)