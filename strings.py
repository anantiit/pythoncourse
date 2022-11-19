age = 30
age_str = str(age)
# msg = "you are "+age (invalid)
# print(msg)
msg = "you are " + age_str
print(msg)
print(f"You are {age}")
msg2 = f"You are {age}"
age = 40
# age does not change in below print stmt
print(msg2)
msg1 = "afaf\"quote check with in string\"adsfa"
print(msg1)

msg1 = "afaf'quote check with in string'adsfa"
print(msg1)

msg1 = 'afaf\"quote check with in string\"adsfa'
print(msg1)

# msg1 = 'afaf"quote check with in string"adsfa' is invalid
print(msg1)

# String format function
friend_name = "prakash"
greeting = "Hello! How are you {name}"
friend_greeting = greeting.format(name=friend_name)
print(friend_greeting)
