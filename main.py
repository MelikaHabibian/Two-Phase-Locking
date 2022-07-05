import re

transactions = []  #List for storing all Transaction Elemenst
output_file = open("output1.txt", 'w')
with open('input1.txt','r') as fp:  #reading the input file
    for line in fp.readlines():
        
        transactions.append(line)
        transactions = [tr.split(';')[0] for tr in transactions]
         
print(transactions)


Transaction_table=[] #Transaction table as list
lock_table=[]   #lock_table as list
timestamp=1

def begin_transaction(tr):  #when a it enounter begin Transaction 'b',this function will be called.
    global timestamp
    contents=[(tr[1]),timestamp,'active',[],[]] #storing each Transaction the list
    timestamp=timestamp+1
    Transaction_table.append(contents) #appending list to the Transaction table
    print("Begin transaction: T"+str(tr[1]))
    output_file.write("Begin transaction: T"+str(tr[1]))
    output_file.write('\n')

    
def read_transaction(tr):
    # Data_item - data item to be read
    # Tansaction_id - transaction id of the transaction performing the read operation
    global Transaction_table
    global lock_table
    index_tr=0   #index for Transaction_table
    index_lt=0  #index for Lock_table
    item_found=0
    timestamp1=0
    timestamp2=0
    transaction_found=0
    Transaction_id=str(tr[1])
    Data_item=tr.split('(')[1][0]
   # print(Transaction_id)
    for i in range(len(Transaction_table)):
        #checking the status of requesting Transaction if it is blocked,append to the waitlisting for execution.
        if((Transaction_table[i][0]==Transaction_id) and (Transaction_table[i][2]=='Blocked')):
            Transaction_table[i][4].append(tr)
         #checking the status ,if Transaction is active then perform the Read request based on certain conditions.   
        if((Transaction_table[i][0]==Transaction_id) and (Transaction_table[i][2]=='active')):
            transaction_found=1
            index_tr=i
            
            if len(lock_table)==0: #if no data_items present in the Lock_table
                contentslock=[Data_item,tr[0],[tr[1]]] 
                lock_table.append(contentslock)  #append the data_items to the lock_table
                print("ITEM "+str(Data_item)+" is read locked by T"+str(tr[1]))
                output_file.write("ITEM "+str(Data_item)+" is read locked by T"+str(tr[1]))
                output_file.write('\n')
            else:
                for i in range(len(lock_table)):#if Data_item present
                    f=len(lock_table)
                    if(i>=f):
                        return 0
                    if lock_table[i][0]==Data_item:
                        item_found=1  
                        index_lt=i
                        #if requesting Data_item is in 'Readmode' already locked by different Transaction,append to the list requesting Read
                        if lock_table[index_lt][1]=='r' and Transaction_id not in lock_table[index_lt][2]:
                            lock_table[index_lt][2].append(tr[1])
                            print("T"+str(tr[1])+" is added to the read lock list of Data item "+str(Data_item))
                            output_file.write("T"+str(tr[1])+" is added to the read lock list of Data item "+str(Data_item))
                            output_file.write('\n')
                        #if the requesting data_item is in 'Readmode' and Same Transaction ID'    
                        elif lock_table[index_lt][1]=='r' and Transaction_id in lock_table[index_lt][2]:
                            print("The Requesting Data item already Read locked.")
                        #if the requesting Data_item is 'writemode' locked by same Transaction ID'    
                        elif lock_table[index_lt][1]=='w' and Transaction_id in lock_table[index_lt][2]:
                            print("The Requesting Data item is write locked by the Same Transaction")
                        #if the requesting Data_item is 'writemode' locked by differnt Transaction ID' 
                        #then cautious wait protocol is implemented by comparing the timestamps of Transactions.
                        elif lock_table[index_lt][1]=='w' and Transaction_id not in lock_table[index_lt][2]:#cautious wait protocol
                            t1=Transaction_table[index_tr][0]
                            t2=lock_table[index_lt][2][0]
                            for sublist in Transaction_table:
                                if(sublist[0]==t2):
                                    status_t2=sublist[2]

                            if(status_t2=="blocked"): # Transaction holding the lock is in the block state, hence we abort the requesting transaction.
                                print("Abort T"+str(t1)+" as T"+str(t2)+" is blocked.")
                                output_file.write("Abort T"+str(t1)+" as T"+str(t2)+" is blocked.")
                                output_file.write('\n')
                                Abort_transaction(t1)
                 
                            elif(status_t2!="blocked"):# Transaction holding the lock is not in block state, hence we block requesting transaction.
                                print("BLOCK T"+str(t1)+" as ITEM "+str(Data_item)+" is held by T"+str(t2))
                                output_file.write("BLOCK T"+str(t1)+" as ITEM "+str(Data_item)+" is held by T"+str(t2))
                                output_file.write('\n')
                                block_transaction(t1,t2,tr)
                                
                if(item_found==0): #if no Data_item found ,then Directly append to the lock_table with mode'R'.
                    print("ITEM "+str(Data_item)+" is read locked by T"+str(tr[1]))
                    output_file.write("ITEM "+str(Data_item)+" is read locked by T"+str(tr[1]))
                    output_file.write('\n')
                    contentslock=[Data_item,tr[0],[tr[1]]] 
                    lock_table.append(contentslock)

    if(transaction_found==0):
        print("T"+Transaction_id+" hasn't begun or is not in active state")
        output_file.write("T"+Transaction_id+" hasn't begun or is not in active state")
        output_file.write('\n')

def write_transaction(tr): #if it encounter 'w' variable with the Data_item
    global Transaction_table
    global lock_table
    index_tr=0
    index_lt=0
    item_found=0
    timestamp1=0
    timestamp2=0
    transaction_found=0
    Transaction_id=str(tr[1])
    Data_item=tr.split('(')[1][0]
    
    
    for i in range(len(Transaction_table)):
        #if Transaction is blocked,append to the list of waiting operations
        if((Transaction_table[i][0]==Transaction_id) and (Transaction_table[i][2]=='Blocked')):
            Transaction_table[i][4].append(tr)
         #if Transaction is active status then following 'write' request is permitted based on following conditions
        if((Transaction_table[i][0]==Transaction_id) and (Transaction_table[i][2]=='active')):
            transaction_found=1
            index_tr=i
            
            if len(lock_table)==0: #if no data_items present in the Lock_table
                print("lock table empty, please read the data item first")
            else:
                for i in range(len(lock_table)):#if Data_item is found
                    f=len(lock_table)
                    if(i>=f):
                        return 0
                    if lock_table[i][0]==Data_item:
                        item_found=1
                        index_lt=i
                        #If reuesting Data_item is write locked by same Transaction
                        if lock_table[index_lt][1]=='w' and Transaction_id in lock_table[index_lt][2]:
                            print("This Data item has already been write locked by this transaction")
                       #if requesting Data_item is write locked by different Transaction
                        elif lock_table[index_lt][1]=='w' and Transaction_id not in lock_table[index_lt][2]:# cautious wait protocol
                            t1=Transaction_table[index_tr][0]
                            t2=lock_table[index_lt][2][0]

                            for sublist in Transaction_table:
                                    if(sublist[0]==t2):
                                        status_t2=sublist[2]
                                    
                            if(status_t2=="blocked"): # Transaction holding the lock is in the block state, hence we abort the requesting transaction.
                                print("Abort T"+str(t1)+" as T"+str(t2)+" is blocked.")
                                output_file.write("Abort T"+str(t1)+" as T"+str(t2)+" is blocked.")
                                output_file.write('\n')
                                Abort_transaction(t1)
                 
                            elif(status_t2!="blocked"):# Transaction holding the lock is not in block state, hence we block requesting transaction.
                                print("BLOCK T"+str(t1)+" as ITEM "+str(Data_item)+" is held by T"+str(t2))
                                output_file.write("BLOCK T"+str(t1)+" as ITEM "+str(Data_item)+" is held by T"+str(t2))
                                output_file.write('\n')
                                block_transaction(t1,t2,tr)
                                
                        #if requesting Data_item by the Transaction is not Read locked first
                        elif lock_table[index_lt][1]=='r' and Transaction_id not in lock_table[index_lt][2]:
                            print("Read lock the data item first")
                        #if requesting data_item is Read locked by the same Transaction,then update to 'write' mode
                        elif lock_table[index_lt][1]=='r' and Transaction_id in lock_table[index_lt][2]:
                            if len(lock_table[index_lt][2])==1:#checking only one Transaction has read lock
                                lock_table[index_lt][1]='w'
                                print("Read lock upgraded to write lock on ITEM "+str(Data_item)+" by T"+str(tr[1]))
                                output_file.write("Read lock upgraded to write lock on ITEM "+str(Data_item)+" by T"+str(tr[1]))
                                output_file.write('\n')
                            
                            elif (len(lock_table[index_lt][2])>1):#if two many Transactions
                                t1=Transaction_id
                        
                                for lock_table_tid in lock_table[index_lt][2]:
                                    if(lock_table_tid!=Transaction_id):
                                        t2=lock_table_tid
                                        
                                        for sublist in Transaction_table:#iterrate through for loop to compare timestamps to implement cautious wait protocol
                                            if(sublist[0]==t2):
                                                status_t2=sublist[2]
                                        
                                        if(status_t2=="Blocked"): # Transaction holding the lock is in the block state, hence we abort the requesting transaction.
                                            print("Abort T"+str(t1)+" as T"+str(t2)+" is blocked.")
                                            output_file.write("Abort T"+str(t1)+" as T"+str(t2)+" is blocked.")
                                            output_file.write('\n')
                                            Abort_transaction(t1)
                                            
                 
                                        elif(status_t2!="blocked"):# Transaction holding the lock is not in block state, hence we block requesting transaction.
                                            print("BLOCK T"+str(t1)+" as ITEM "+str(Data_item)+" is held by T"+str(t2))
                                            output_file.write("BLOCK T"+str(t1)+" as ITEM "+str(Data_item)+" is held by T"+str(t2))
                                            output_file.write('\n')
                                            block_transaction(t1,t2,tr)
                                                                       
                if(item_found==0): #no data_item found the requesting Transaction should 'Read' lock it first
                    print("item not in lock table")
                    print("Read lock the data item first")

    if(transaction_found==0):
        print("T"+Transaction_id+" hasn't begun or is not in active state")
        output_file.write("T"+Transaction_id+" hasn't begun or is not in active state")
        output_file.write('\n')

def commit_transaction(tr):
    global Transaction_table
    global lock_table
    Transaction_id=str(tr[1])
    blocked_operations=[]
    commit_status=0
    
    for i in range(len(Transaction_table)):
        #if Transaction is blocked,append to the list of waiting operations
        if((Transaction_table[i][0]==Transaction_id) and (Transaction_table[i][2]=='Blocked')):
            Transaction_table[i][4].append(tr)

    for sublist in Transaction_table:
        if(sublist[0]==Transaction_id):
            if(sublist[2]=='active'):
                commit_status=1

    if(commit_status==1):
        
        # tid - transaction id of the transaction to be commited

        # Update the lock table by removing all the locks being hold by the transaction to be committed
    
        #reading the items of the lock table

        for sublist in reversed(lock_table):
            for lock_table_tid in sublist[2]:

                # checking if the transaction to be committed is holding locks on any data item
                if Transaction_id == lock_table_tid:

                    # checking if that data item has only one lock

                    if len(sublist[2])==1:
                        # removing the complete entry(data item, lock mode, t-id)
                        lock_table.remove(sublist)
                    # checking if that data item also has locks by other transactions
                    else:
                        # removing t-id of only the committing transaction
                        sublist[2].remove(Transaction_id)
 
        # Updating the transaction table 

        # Reading the items of the transaction table

        for i in range(len(Transaction_table)):
            if Transaction_table[i][0]==Transaction_id:
                # Updating the transaction table status to 'Committed'
                Transaction_table[i][2]='Committed'

            # Checking if any transaction is blocked by the committing transaction
            if(Transaction_table[i][2]=='Blocked'):
                for x in Transaction_table[i][3]:
                    if(x==Transaction_id):

                        # Executing the next blocked transaction and changing it's status to 'Active'
                        Transaction_table[i][2]='active'
                        Transaction_table[i][3] = []
                        for operation in Transaction_table[i][4]:
                            blocked_operations.append(operation)
                        Transaction_table[i][4]=[]
        
        print("Transaction T"+str(tr[1])+" committed successfully")
        output_file.write("Transaction T"+str(tr[1])+" committed successfully")
        output_file.write('\n')

        for operation in blocked_operations:
            execute_transaction(operation)
    
    else:
        print("Transaction is in a state of Abort or Block")
        output_file.write("Transaction is in a state of Abort or Block")
        output_file.write('\n')

    

def block_transaction(blocked_tid,blocked_by_tid,blocked_operation):

    global Transaction_table
    blocked_by_tid_list=[]

    # blocked_tid - transaction id of the transaction to be blocked
    # blocked_by_tid - transaction id of the transaction which is forcing the block 
    # blocked_operation - operation to be blocked

    # Updating the transaction table 
    # Reading the items of the transaction table

    for i in range(len(Transaction_table)):
        if Transaction_table[i][0] == blocked_tid:
            if Transaction_table[i][2] == 'active':
                # Updating the transaction table status to 'blocked'
                Transaction_table[i][2] = 'Blocked'
                
                # Updating the 'blocked by' list for that transaction
                Transaction_table[i][3].append(blocked_by_tid)

                # Storing the operation in the list 'Blocked Operations'
                Transaction_table[i][4].append(blocked_operation)

            if blocked_by_tid not in Transaction_table[i][3]:
                Transaction_table[i][3].append(blocked_by_tid)

def Abort_transaction(abort_tid):

    global Transaction_table
    global lock_table
    blocked_operations=[]
    
    for i in range(len(Transaction_table)):

        if Transaction_table[i][0]==abort_tid:
            
            if(Transaction_table[i][2]!='committed' and Transaction_table[i][2]!='aborted'):
                
                #updating the Transaction_table

                Transaction_table[i][2]='aborted'
                Transaction_table[i][3]=[]
                Transaction_table[i][4]=[]

                #updating the lock_table

                for i in range(len(lock_table)):
                    if abort_tid in lock_table[i][2]:
                        if len(lock_table[i][2])==1:
                            lock_table.remove(lock_table[i])
                        else:
                            lock_table[i][2].remove(abort_tid)

    for i in range(len(Transaction_table)):
        for x in Transaction_table[i][3]:
            if abort_tid==x :

                Transaction_table[i][3].remove(abort_tid)

            # Checking if any transaction is blocked by the aborting transaction
                if(Transaction_table[i][2]=='Blocked' and len(Transaction_table[i][3])==0):

                # Executing the next blocked transaction and changing it's status to 'Active'
                    Transaction_table[i][2]='active'
                    Transaction_table[i][3] = []
                for operation in Transaction_table[i][4]:
                    blocked_operations.append(operation)
                Transaction_table[i][4]=[]

    for operation in blocked_operations:
        execute_transaction(operation)

def execute_transaction(tr):
        output_file.write('Operation is: '+str(tr))
        output_file.write('\n')
        print("Operation is",tr)
        if tr[0]=='b':
            begin_transaction(tr)
        if tr[0]=='r':
            read_transaction(tr)
        if tr[0]=='w':
            write_transaction(tr)
        if tr[0]=='e':
            commit_transaction(tr) 
        
        output_file.write('Transaction Table: '+str(Transaction_table))
        output_file.write('\n')
        output_file.write('Lock Table: '+str(lock_table))
        output_file.write('\n')
        output_file.write('\n')
        print("Transaction Table: ",Transaction_table)
        print("Lock Table: ",lock_table)
        print('\n')


for tr in transactions:
    execute_transaction(tr)

output_file.close()
