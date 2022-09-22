#arr is assumed to be sorted in ascending order
def binary_insert(arr, p):
    if len(arr) == 0:
        arr.append(p)
        return arr
    l = 0
    r = len(arr) - 1
    while l < r:
        m = (l + r)//2
        if arr[m] > p:
            r = m - 1
            continue
        if arr[m] < p:
            l = m + 1
            continue
        return arr[:m] + [p] + arr[m:]
        return
    if arr[l] > p:
        return arr[:l] + [p] + arr[l:]
    else:
        return arr[:l+1] + [p] + arr[l+1:]
    return
