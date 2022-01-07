class Utils:
    @staticmethod
    def flatten_dict(d,sep="-->",prefix="",_basek=True):
        flat_dict={}
        if prefix !="" and isinstance(d,(dict)):
            prefix=f"{prefix}{sep}"
        if isinstance(d,dict):
            for k,v in d.items():
                #print("working on ",k,v)
                #print("prefix is ",prefix)
                if isinstance(v,dict):
                    
                    flat_dict.update(Utils.flatten_dict(v,sep=sep,prefix=prefix+k,_basek=False))
                elif isinstance(v,(list, tuple)):
                    flat_dict.update(Utils.flatten_dict(v,sep=sep,prefix=prefix+f"{k}",_basek=False))
                else:
                    flat_dict[f"{prefix}{k}"]=v
                if _basek:
                    prefix=""
        elif isinstance(d,(list, tuple)):
            #print("prefix ",prefix)
            for n_i in range(len(d)):
                flat_dict.update(Utils.flatten_dict(d[n_i],sep=sep,prefix=prefix+f"[{n_i}]",_basek=False))
        else:
            flat_dict[f"{prefix}"]=d
        #print("returning ",flat_dict)
        return flat_dict

    