def orderFood(isVegetarian,doLikePizza,isOnlineOrder,maxFare):
    if isOnlineOrder:
        print("Ordering online")
    else:
        print("Going to restaurant")
    if isVegetarian and doLikePizza:
        print("Show only veg menu but not pizza")
    elif isVegetarian and onlyIndian:
        print("Show only veg indian menu")
    else:
        print("show all")


isVegetarian=True
doLikePizza=False
isOnlineOrder=False
onlyIndian=True
maxFare=50

orderFood(isVegetarian,doLikePizza,isOnlineOrder,maxFare)
