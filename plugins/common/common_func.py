def get_sftp():
    print('sftp 작업을 시작합니다')

def regist(name,sex,*args):
    print(f'이름: {name}')
    print(f'성별: {sex}')
    print(f'기타옵션들: {args}') 

def regist2(name,sex,**kwargs):
    #print(f'기타옵션들: {args}')
    print(f'이름:{name}')
    print(f'성별:{sex}')
    email = kwargs['email'] or None
    phone = kwargs['phone'] or None
    
    if email:
        print(email)
    if phone:
        print(phone)
