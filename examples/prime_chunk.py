# Prime sieve for a chunk of numbers [START, END).
# Each task node gets a different chunk.
# Together they cover all numbers up to 1,000,000.
#
# Example distribution (chunk_size=1000):
#   Task 1: START=2,    END=1002
#   Task 2: START=1002, END=2002
#   ...
#   Task 1000: START=999000, END=1000001
#
# Output: comma-separated primes in range (deterministic -> consensus works)

START = 2
END = 1002  # exclusive

primes = []
for n in range(max(2, START), END):
    if n < 2:
        continue
    ok = True
    for i in range(2, int(n ** 0.5) + 1):
        if n % i == 0:
            ok = False
            break
    if ok:
        primes.append(n)

print(",".join(map(str, primes)))
