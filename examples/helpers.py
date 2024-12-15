from jaf.jaf_eval import jaf_eval

def main():
    obj = {
        'a': 1,
        'b': 2,
        'c': {
            'd': 3,
            'e': 4,
            'f': {
                'g': 5,
                'h': 6
            }
        }
    }
    a = jaf_eval.get_val(obj, 'a')
    print('a', a)
    c = jaf_eval.get_val(obj, 'c')
    print('c', c)
    d = jaf_eval.get_val(obj, 'c.d')    
    print('c.d', d)


    jaf_eval.eval_oper('a', obj)

if __name__ == "__main__":
    main()
