from rediscluster import RedisCluster
import os

def main():
    which_file = os.getenv('which_file')

    # connect to ElastiCache Redis Cluster
    redis = RedisCluster(startup_nodes=[
        {"host": "deep-redis-elasticache-cluster.f3inzj.clustercfg.use1.cache.amazonaws.com", "port": "6379"}],
                                      decode_responses=True, skip_full_coverage_check=True)

    if which_file == 'file1':
        with open('elasticache_data.csv','r') as inputfile1:
            for i in inputfile1:
                user_dict = dict()
                user_id,first_name,last_name,country,state_NULL,zip_NULL,gender,credit_card_no,curerncy,\
                user_image_url, phone_no = i.split(',')

                key = user_id
                user_dict['first_name'] = first_name
                user_dict['last_name'] = last_name
                user_dict['country'] = country
                user_dict['gender'] = gender
                user_dict['credit_card_no'] = credit_card_no
                user_dict['curerncy'] = curerncy
                user_dict['user_image_url'] = user_image_url
                user_dict['phone_no'] = phone_no

                # Write to Redis Cluster
                print(key,user_dict)
                #redis.hmset(key, user_dict)


    if which_file == 'file2':
        with open('RETAIL_CART_DATA.csv','r') as inputfile2:
            for j in inputfile2:
                cart_dict = dict()
                user_id,status,date,vin, first_name, last_name = j.split(',')
                key = user_id
                cart_dict['status'] = status
                cart_dict['date'] = date
                cart_dict['vin'] = vin
                cart_dict['first_name'] = first_name
                cart_dict['last_name'] = last_name

                # Write to Redis Cluster
                redis.hmset(key, cart_dict)


if __name__ == '__main__':
    main()
