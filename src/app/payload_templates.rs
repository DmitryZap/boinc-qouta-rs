pub(super) const GPU_PYTORCH_TEMPLATE: &str = "\
import torch

device = 'cuda' if torch.cuda.is_available() else 'cpu'
print(f'device={device}')

a = torch.randn(512, 512, device=device)
b = torch.randn(512, 512, device=device)
c = torch.mm(a, b)

checksum = float(c.sum().cpu())
print(f'matmul_512x512_checksum={checksum:.4f}')
";

pub(super) const PRIME_CHECK_TEMPLATE: &str = "\
N = 982451653  # change per task

def is_prime(n):
    if n < 2: return False
    if n == 2: return True
    if n % 2 == 0: return False
    i = 3
    while i * i <= n:
        if n % i == 0: return False
        i += 2
    return True

print(f\"{N}:{'prime' if is_prime(N) else 'composite'}\")\
";

pub(super) const PRIME_CHUNK_TEMPLATE: &str = "\
START = 2
END = 1002  # exclusive - change per task

primes = []
for n in range(max(2, START), END):
    ok = all(n % i != 0 for i in range(2, int(n**0.5)+1))
    if ok:
        primes.append(n)
print(','.join(map(str, primes)) or 'none')\
";
