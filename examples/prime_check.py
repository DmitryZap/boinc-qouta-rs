# Single number primality check
# Each task node checks one number.
# Output is deterministic -> consensus works across workers.
#
# To distribute: submit one task per number N (or per chunk).
# Change N below before submitting.

N = 982451653  # a large prime — change per task

def is_prime(n):
    if n < 2:
        return False
    if n == 2:
        return True
    if n % 2 == 0:
        return False
    i = 3
    while i * i <= n:
        if n % i == 0:
            return False
        i += 2
    return True

result = "prime" if is_prime(N) else "composite"
print(f"{N}:{result}")
