name="joerge"
age=70
isMale=True
print("there is a man "+name.capitalize()+",",len(name))
print("he is "+str(age)+"years old")
print("he likes his name "+name+",")
print("But he does not like being ",age)
if isMale:
    print("\n\"he is a man")
else:
    print("she is women\"")

print("His name starts with ",name[0])
print("his name has character at index",name.index("g"))
print("his name replaced",name.replace("j","G"))
#print("his name has character at index",name.index("c"))
