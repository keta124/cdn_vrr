import json
from math import log


class Network(object):
        """docstring for Network"""
        def __init__(self):
                super(Network, self).__init__()
                pass
        def subnet_to_number(self,subnet):
            subnet = subnet.replace('/','.')
            list_subnet =  map(int,subnet.split('.'))
            if len(list_subnet)==4:
                    list_subnet.append(32)
            value = list_subnet[0]*256*256*256+list_subnet[1]*256*256+list_subnet[2]*256+list_subnet[3]
            value_min = value - value%(2**(32-list_subnet[4]))
            value_max = value_min + 2**(32-list_subnet[4]) -1
            number_turple = (value_min,value_max)
            return number_turple

        def number_to_subnet(self,number_turple):
            subnet_ = number_turple[1]-number_turple[0]+1
            subnet = 32 -int(log(subnet_)/log(2))
            if number_turple[1] == (number_turple[0]+2**(32-subnet) -1):
                D = number_turple[0] % 256
                C = ((number_turple[0]-D)/256)%256
                B = ((number_turple[0]-D-C*256)/(256*256))%256
                A = (number_turple[0]-D-C*256-B*256*256) / (256*256*256)
                subnet_str = str(A)+"."+str(B)+"."+str(C)+"."+str(D)+"/"+str(subnet)
                return subnet_str
            else:
                return '0.0.0.0'
        def check_subnet(self,subnet_sub, subnet):
            try:
                subnet_sub_number = self.subnet_to_number(subnet_sub)
                subnet_number = self.subnet_to_number(subnet)
                if (subnet_sub_number[0] >= subnet_number[0]) and (subnet_sub_number[1] <= subnet_number[1]):
                        return True
                else:
                        return False
            except:
                return False
        def devide_subnet(self,network,intsub):
            network_number = self.subnet_to_number(network)
            network_ = network.replace('/','.')
            list_subnet =  map(int,network_.split('.'))
            if intsub>=list_subnet[4]:
                    net_long = 2**(32-intsub)
                    network_number_ =[0,0]
                    network_number_[0] =  network_number[0]
                    network_number_[1] =  network_number[0]-1

                    list_subnet =[]
                    while network_number_[1]<network_number[1]:
                            network_number_[0] = network_number_[1]+1
                            network_number_[1] =  network_number_[0] + net_long -1
                            subnet = self.number_to_subnet(network_number_)
                            list_subnet.append(subnet)
                    return list_subnet
            else:
                    net_long = 2**(32-intsub)
                    network_number_ =[0,0]
                    network_number_[0]= network_number[0] - network_number[0]%(2**intsub)
                    network_number_[1]= network_number_[0] + net_long -1
                    return [self.number_to_subnet(network_number_)]
'''

def convertSubnet(network,intsub):
        network_Number = subnetToNumBer(network)
        network_ = network.replace('/','.')
        list_subnet =  map(int,network_.split('.'))
        netLong = 2**(32-intsub)
        network_Number_ =[0,0]
        network_Number_[0] =  network_Number[0]
        network_Number_[1] =  network_Number[0]-1

        list_subnet =[]
        while network_Number_[1]<network_Number[1]:
                network_Number_[0] = network_Number_[1]+1
                network_Number_[1] =  network_Number_[0] + netLong -1
                subnet = numberToSubnet(network_Number_)
                list_subnet.append(subnet)
        return list_subnet

def subnet_20(ip):
        listip =  map(int,ip.split('.'))
        value = listip[0]*256*256*256+listip[1]*256*256+listip[2]*256+listip[3]
        value_ = value - value % 4096
        C = (value_/256)%256
        B = ((value_-C*256)/(256*256))%256
        A = (value_-C*256-B*256*256) / (256*256*256)
        subnet = str(A)+"."+str(B)+"."+str(C)+".0/20"
        return subnet
def replaceSpace(str):
        while ' ' in str:
                str=str.replace(' ','')
        return str
if __name__ == "__main__":
        g = open ('ipasp_done_24.csv','wb+')
        writer = csv.writer(g, delimiter=',')
	with open ('ipasp.csv','rb') as f:
                reader = csv.reader(f)
                reader_list = list(reader)
        list_network =[]
        for i in reader_list :
                list_ = devideSubnet(replaceSpace(i[2]),24)
                for j in list_:
                        write = [i[1],j]
                        list_network.append(write)
        list_network_ =[]
        for line in list_network:
                writer.writerow(line)

if __name__ == '__main__':
    nw =Network()
    print nw.devide_subnet('192.168.42.0/20',19)
'''