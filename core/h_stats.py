# this borrows from data_manipulation project 12-7-20
from scipy.stats import chi2

def resid_chisq(o, expected):
    a = 0
    if expected > 0:
        a = (o - expected) * (o - expected) / expected
    return a


def chisq_2_2(g1pos, g1neg, g2pos, g2neg):
    row1 = g1pos + g1neg
    row2 = g2pos + g2neg
    col1 = g1pos + g2pos
    col2 = g1neg + g2neg

    n = row1 + row2

    ea = col1 * row1 / n
    eb = col2 * row1 / n
    ec = col1 * row2 / n
    ed = col2 * row2 / n

    a = resid_chisq(g1pos, ea)
    b = resid_chisq(g1neg, eb)
    c = resid_chisq(g2pos, ec)
    d = resid_chisq(g2neg, ed)

    chisq = a + b + c + d
    a = chi2.pdf(chisq, 1)
    if a > 1:
        a = 1
    return chisq, a
