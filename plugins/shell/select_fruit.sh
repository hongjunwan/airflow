FRUIT=$1
if [ $FRUIT == APPLE ];then
	echo "you selected Apple!"
elif [ $FRUIT == ORANGE ];then
	echo "you selected Orange!"
elif [ $FRUIT == GRAPE ];then
	echo "you selected Grape!"
else
	echo "you selected other Fruit!"
fi
