

def cookies_str_2_dict(cookies_str):
	cookies = [b'BAIDUID=C68C1ACE359009A9063A5F6B4B863CE7:FG=1; max-age=31536000; expires=Fri, 09-Aug-19 11:53:46 GMT; domain=.baidu.com; path=/; version=1', b'MCITY=deleted; expires=Wed, 09-Aug-2017 11:53:45 GMT; path=/; domain=baidu.com']
	cookies_list = list()
	for c in cookies:
		cookies_dict = dict()
		c_split = c.decode('utf-8').split(';')
		name, value = c_split[0].split('=', 1)
		cookies_dict['name'] = name
		cookies_dict['value'] = value
		for component in c_split[1:]:
			k, v = component.split('=')
			cookies_dict[k.replace(' ', '')] = v
		cookies_list.append(cookies_dict)

def merge_cookies():
	pass

class Test(object):
	def __init__(self):
		self
if __name__ == '__main__':
	

	print(cookies_list)