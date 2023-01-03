def reverseList(A,start,end):
    while start<end:
        A[start],A[end] = A[end], A[start]
        start=start+1
        end=end-1
def reorderArray(A,index,n):
    for j in range(n):
        print(A)
        i=j
        print(i)
        while index[i]!=i:
          print(A)
          print(index)
          temp = A[i]
          A[i]=A[index[i]]
          A[index[i]]=temp

          temp = index[i]
          index[i]= index[index[i]]
          index[temp]=temp


def reorderArray1(A,index,n):
    for i in range(n):
          temp = A[i]
          A[i]=A[index[i]]
          A[index[i]]=temp

          temp = index[i]
          index[i]= index[index[i]]
          index[temp]=temp
          print(A)
          print(index)


friends=["naidu","laxman","janakiram","sagar"]
more=["nagi"]
friends.extend(more)
friends.append("2")
friends.insert(1,"harish")
friends.remove("sagar")
print(friends.index("naidu"))
for val in friends:
    print(val)
#print(friends.index("sagar"))

list=[3,4,1,3,5]
print(list)
A=[4,4,3,2,7,1,9]
index=[3,1,0,2,6,5,4]
reorderArray1(A,index,7)
print(A)
print(index)
# print(A[::-1])
# print("reversed list is:")
# reverseList(A,0,6);
# print(A)
# print(A[::-1])

