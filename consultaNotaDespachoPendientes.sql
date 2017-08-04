SELECT AVMOV_MovNotaDespachoCab.id_MovValeCab AS idND, 
AVMOV_MovNotaDespachoCab.fec_vale AS fecVale, 
AVMOV_MovNotaDespachoCab.num_vale AS numVale, 
AVMOV_MovNotaDespachoCab.cod_persona AS codPersona, 
agmae_persona.num_identificacion AS rucPersona,
agmae_persona.Nom_Razon_Social AS nomPersona, 
AVMOV_MovNotaDespachoDet.ruc_Companyia AS rucCompanya, 
agmae_companyias.des_companyia AS nomCompanya, 
AVMOV_MovNotaDespachoDet.flg_estado_factura AS codEstFactura,  
IIf(AVMOV_MovNotaDespachoDet.flg_estado_factura=1,'true','false') AS nomEstFactura,  
IIF(Count(AVMOV_MovNotaDespachoDet.flg_estado_factura)=IIf(AVMOV_MovNotaDespachoDet.flg_estado_factura=1, 
                                                           Count(AVMOV_MovNotaDespachoDet.flg_estado_factura),0),'FACTURADO', 
    IIF(Count(AVMOV_MovNotaDespachoDet.flg_estado_factura)=
        IIf(AVMOV_MovNotaDespachoDet.flg_estado_factura=0 ,Count(AVMOV_MovNotaDespachoDet.flg_estado_factura),0),'PENDIENTE', 'PARCIAL') )
        AS desEstadoFactura FROM ((AVMOV_MovNotaDespachoDet INNER JOIN AVMOV_MovNotaDespachoCab ON AVMOV_MovNotaDespachoDet.id_MovValeCab = AVMOV_MovNotaDespachoCab.id_MovValeCab) 
                                  LEFT JOIN agmae_persona ON AVMOV_MovNotaDespachoCab.cod_persona = agmae_persona.cod_persona) LEFT JOIN agmae_companyias ON AVMOV_MovNotaDespachoDet.ruc_Companyia = agmae_companyias.ruc_companyia GROUP BY AVMOV_MovNotaDespachoCab.id_MovValeCab, AVMOV_MovNotaDespachoCab.fec_vale, AVMOV_MovNotaDespachoCab.num_vale, AVMOV_MovNotaDespachoCab.cod_persona, agmae_persona.num_identificacion, agmae_persona.Nom_Razon_Social, AVMOV_MovNotaDespachoDet.ruc_Companyia, agmae_companyias.des_companyia, AVMOV_MovNotaDespachoDet.flg_estado_factura, AVMOV_MovNotaDespachoCab.flg_estado_factura;
