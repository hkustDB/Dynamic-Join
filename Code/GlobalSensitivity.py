import numpy as np

def GS_Q_edge(tau):
    return 1

def GS_Q_two_path(tau):
    return max(tau[('1', 'to')], tau[('2', 'from')])

def GS_Q_three_path(tau):
    GS_1 = tau[('2', 'from')] * tau[('3', 'from')]
    GS_2 = tau[('1', 'to')] * tau[('3', 'from')]
    GS_3 = tau[('1', 'to')] * tau[('2', 'to')]
    return max(GS_1, GS_2, GS_3)

def GS_Q_triangle(tau):
    GS_1 = min(tau[('2', 'from')], tau[('3', 'to')])
    GS_2 = min(tau[('1', 'to')], tau[('3', 'from')])
    GS_3 = min(tau[('2', 'to')], tau[('1', 'from')])
    return max(GS_1, GS_2, GS_3)

def GS_Q_rectangle(tau):
    GS_1 = min(tau[('2', 'from')] * tau[('3', 'from')], tau[('4', 'to')] * tau[('3', 'to')], tau[('2', 'from')] * tau[('4', 'to')])
    GS_2 = min(tau[('3', 'from')] * tau[('4', 'from')], tau[('1', 'to')] * tau[('4', 'to')], tau[('3', 'from')] * tau[('1', 'to')])
    GS_3 = min(tau[('4', 'from')] * tau[('1', 'from')], tau[('2', 'to')] * tau[('1', 'to')], tau[('4', 'from')] * tau[('2', 'to')])
    GS_4 = min(tau[('1', 'from')] * tau[('2', 'from')], tau[('3', 'to')] * tau[('2', 'to')], tau[('1', 'from')] * tau[('3', 'to')])
    return max(GS_1, GS_2, GS_3, GS_4)

def GS_Q_two_star(tau):
    GS_1 = tau[('1', 'from')]
    GS_2 = tau[('2', 'from')]
    return max(GS_1, GS_2)

def GS_Q_three_star(tau):
    GS_1 = tau[('2', 'from')] * tau[('3', 'from')]
    GS_2 = tau[('1', 'from')] * tau[('3', 'from')]
    GS_3 = tau[('1', 'from')] * tau[('2', 'from')]
    return max(GS_1, GS_2, GS_3)

def GS_Q_four_star(tau):
    GS_1 = tau[('2', 'from')] * tau[('3', 'from')] * tau[('4', 'from')]
    GS_2 = tau[('1', 'from')] * tau[('3', 'from')] * tau[('4', 'from')]
    GS_3 = tau[('1', 'from')] * tau[('2', 'from')] * tau[('4', 'from')]
    GS_4 = tau[('1', 'from')] * tau[('2', 'from')] * tau[('3', 'from')]
    return max(GS_1, GS_2, GS_3, GS_4)

def GS_Q_q7(tau):
    GS_1 = tau[('O', 'ck')] * tau[('L', 'ok')] * tau[('S', 'sk')]
    GS_2 = tau[('C', 'ck')] * tau[('L', 'ok')] * tau[('S', 'sk')]
    GS_3 = tau[('C', 'ck')] * tau[('O', 'ok')] * tau[('S', 'sk')]
    GS_4 = tau[('C', 'ck')] * tau[('O', 'ok')] * tau[('L', 'sk')]
    return max(GS_1, GS_2, GS_3, GS_4)

def GS_Q_q9(tau):
    GS_1 = tau[('PS', 'sk')] * tau[('L', 'sk', 'pk')] * tau[('O', 'ok')]
    GS_2 = tau[('S', 'sk')] * tau[('L', 'sk', 'pk')] * tau[('O', 'ok')]
    GS_3 = tau[('S', 'sk')] * tau[('PS', 'sk', 'pk')] * tau[('O', 'ok')]
    GS_4 = tau[('S', 'sk')] * tau[('PS', 'sk', 'pk')] * tau[('L', 'ok')]
    return max(GS_1, GS_2, GS_3, GS_4)