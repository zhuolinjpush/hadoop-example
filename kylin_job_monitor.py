# -*- coding:utf-8 -*-
import sys
import requests
reload(sys)
sys.setdefaultencoding('utf-8')

HEADERS={'Authorization': 'Basic 123456=='}
CONTENT_TYPE={'Content-Type': 'application/json'}
CUBE_LIST_URL='http://172.0.0.1:7070/kylin/api/cubes'
CUBE_JOB_LIST='http://172.0.0.1:7070/kylin/api/jobs'
ALARM_URL='http://localhost/v1/alert/'
PROJECT_NAME='Pro_Push_Kpi'

def get_all_cube_name(s):
	try:
		r = s.get( CUBE_LIST_URL, headers = HEADERS )
		print r.url
		cubes = []
		for j in r.json():
			cubes.append(j['name'])
		return cubes
	except Exception,e:
		print e
		alarm(178, 'kylin monitor error, please check!')	

def get_cube_jobs(s, cube, error_cubes, overtime_cubes):
	try:
		params_str = '?projectName=' + PROJECT_NAME + '&cubeName=' + cube + '&offset=0&limit=1000&status=2,8&timeFilter=0'
		print params_str
		r = s.get( CUBE_JOB_LIST + params_str, headers = HEADERS )
		print r.url
		for j in r.json():
			job_status = j['job_status']
			if job_status == "RUNNING" :
				name = j['name']
				duration = j['duration']
				cost = duration / 60 
				if cost > 60 :
					arr = str(name).split(' ')
					seg = '%s_%s' % (arr[0],arr[2])
					content = '%s cost %s min' % (seg, cost)				
					overtime_cubes.append(content)
			elif job_status == "ERROR" :
				name = j['name']
				error_cubes.append(str(name))
			else :
				print 'other job status, %s' % job_status
	except Exception,e:
		print e

def alarm(code, msg):
	s = requests.session()
	s.headers.update( CONTENT_TYPE )
	json_str = '{"code":' + str(code) + ', "desc":"' + msg + '"}'
	r = s.post(ALARM_URL, data=json_str)
	print r

def process():
	error_cubes = []
	overtime_cubes = []
	s = requests.session()
	s.headers.update( CONTENT_TYPE )
	cubes = get_all_cube_name(s)
	for cube in cubes :
		get_cube_jobs(s, cube, error_cubes, overtime_cubes)
	#get_cube_jobs(s, 'Cube_Active_User_Uid', error_cubes, overtime_cubes)
	msg = ''
	if len(error_cubes) > 0:
		msg = 'Error-Cube:%s  ' % error_cubes
	if len(overtime_cubes) > 0:
		msg = '%s %s' % ( msg, overtime_cubes)
	if len(msg) > 300:
		msg = msg[0:300]
	print msg
	if len(msg) > 0:
		alarm(178, msg)
	

if __name__=="__main__":
	print '==================================='
	process()
