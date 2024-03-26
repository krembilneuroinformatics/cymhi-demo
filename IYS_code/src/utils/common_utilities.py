import random as rd
import uuid
import hashlib


# Function to generate unique URIs for form names
rnd = rd.Random()

def generate_uri(nexus_uri_base, seed_value):
    rnd.seed(seed_value)
    random_uuid = uuid.UUID(int=rnd.getrandbits(128), version=4)
    random_uri = f"{nexus_uri_base}{str(random_uuid)}"
    return random_uri

### 2 seed values
# Function to generate unique URIs for subject_ids 
def generate_uri2(nexus_uri_base, seed_value1, seed_value2):
    # Use a combination of the two seed values
    combined_seed = f"{seed_value1}-{seed_value2}"
    
    # Generate a hash value of the combined_seed to ensure a unique but repeatable integer for seeding
    seed_int = int(hashlib.sha256(combined_seed.encode()).hexdigest(), 16) % 10**19
    
    rd.seed(seed_int)
    random_uuid = uuid.UUID(int=rd.getrandbits(128), version=4)
    random_uri = f"{nexus_uri_base}{str(random_uuid)}"
    return random_uri

### 3 Seed values
def generate_uri3(nexus_uri_base, seed_value1, seed_value2,seed_value3):
    
    # Use a combination of the two seed values
    combined_seed = f"{seed_value1}-{seed_value2}-{seed_value3}"
    
    # Generate a hash value of the combined_seed to ensure a unique but repeatable integer for seeding
    seed_int = int(hashlib.sha256(combined_seed.encode()).hexdigest(), 16) % 10**19
    
    rnd.seed(seed_int)
    random_uuid = uuid.UUID(int=rnd.getrandbits(128), version=4)
    random_uri = f"{nexus_uri_base}{str(random_uuid)}"
    return random_uri


### 4 Seed values
def generate_uri4(nexus_uri_base, seed_value1, seed_value2, seed_value3, seed_value4):
    
    # Use a combination of all the seed values
    combined_seed = f"{seed_value1}-{seed_value2}-{seed_value3}-{seed_value4}"
    
    # Generate a hash value of the combined_seed to ensure a unique but repeatable integer for seeding
    seed_int = int(hashlib.sha256(combined_seed.encode()).hexdigest(), 16) % 10**19
    
    rnd.seed(seed_int)
    random_uuid = uuid.UUID(int=rnd.getrandbits(128), version=4)
    random_uri = f"{nexus_uri_base}{str(random_uuid)}"
    return random_uri


def gen_enc_uri(nexus_uri_base, seed_value1, seed_value2, seed_value3, seed_value4,seed_value5, seed_value6, seed_value7, seed_value8, seed_value9,seed_value10,seed_value11,seed_value12):
    combined_seed = f"{seed_value1}-{seed_value2}-{seed_value3}-{seed_value4}-{seed_value5}-{seed_value6}-{seed_value7}-{seed_value8}-{seed_value9}-{seed_value10}-{seed_value11}-{seed_value12}"
    hashed_seed = uuid.uuid5(uuid.NAMESPACE_DNS, combined_seed)
    random_uri = f"{nexus_uri_base}{str(hashed_seed)}"
    return random_uri






