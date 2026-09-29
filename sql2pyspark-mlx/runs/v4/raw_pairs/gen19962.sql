select method from payments where amount > 860 except select method from payments where method <> 'bank_transfer'
