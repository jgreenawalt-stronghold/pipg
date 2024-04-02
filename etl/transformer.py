class Transformer:
    def __init__(self):
        pass

    async def pjm_da_hrl_lmps(self, data):
        value = data[0]['total_lmp_da']
        return value