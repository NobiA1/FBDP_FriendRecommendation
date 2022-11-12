# 作业六伪代码（注释符号为避免markdown识别改为//）
// 采用两轮MapReduce实现简单好友推荐
// 第一轮MapReduce:创建每个id的好友列表
MR_1(path):
    // 读取文件内容
    read(path)
    // list为存储每个用户的<id,friends_id[]>map键值对的vector
    list=vector<map<String,String[]>
    while(!eof())
        do
        string[] a=input.toString().split(" ") // 分空格读取序号写入string数组
        id=a[0]
        friend_id=a[1]
        temp=0 # 用于检查是否已经存在该id的map
        for vec in list # 检查是否已经存在该id的map
            if vec.first==id
                vec.second.append(friend_id) // 添加至friends_id[]
                temp++
                break
        if temp==0
            list.append(<id,friend_id>)
        end
    // 将list内容写入txt
    friend.txt=write(list)         

// 第二轮MapReduce
MR_2():
    read(friend.txt)
    list=vector<map<String,String[]>
    // list为存储每个用户的<id,friends_id[]>map键值对的vector
    while(!eof())
        do
        string[] a=input.toString().split(" ")
        id=a[0]
        // 在list内创建一个新id的map
        friends=<id,[]>
        list.append(friends)
        // 将a的所有friends_id读入map的值
        for(i in (1,a.length()) )
            friends.second.append(a[i])
        end
    // 创建map<id_12,value[]>存储每对用户的推荐value
    recommend=map<id_12,value[]>
    // 避免算两次，定义顺序并拼接(见Concat函数)
    // 完成计算每对用户的value数组
    for (vec in vector)
        id_1=vec.first
        // 好友列表
        friends=vec.second
        // 先读取好友列表写入所有直接好友的value=0
        for (i in friends)
            id_2=i
            id_12=Concat(id_1,id_2)
            // 判断是否之前录入过该对的value
            if(id_12 in recommend.first)
                continue
            else // 否即录入0（直接好友）
                recommend.append(<id_12,[0])
        // 好友列表内部任二为间接好友
        for (id_1 in friends)
            for (id_2 in (friends-id_1))
                id_12=Concat(id_1,id_2)
                // 判断是否之前录入过该对的value
                if(id_12 == recommend[i].first) # 是则在value[]添加1
                    recommend[i].second.append("1")
                else // 否即录入1
                    recommend.append(<id_12,[1]>)    
    // 计算推荐值并输出
    final=map<pair<id_1,id_2>,valueSum>
    for (i in recommend)
        if (0 not in recommend[i].second)
            final.append(Deconcat(id_12),value.sum())
    // 打印final
    print(final)

Concat(id_1,id_2):
    if(id_1>id_2)
        id_12="{id_1}+"."+{id_2}"
    else
        id_12="{id_2}+"."+{id_1}"
    return id_12

Deconcat(id_12)
    id_pair=pair<id_1,id_2>
    id_pair=id_12.split(".")
    return id_pair
    
