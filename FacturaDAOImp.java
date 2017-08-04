package sys.imp;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import sys.dao.FacturaDAO;
import sys.model.AvmovFacturaNdCab;
import sys.model.AvmovFacturaNdDet;
import sys.model.AvmovMovNotaDespachoCab;
import sys.util.Service;

public class FacturaDAOImp implements FacturaDAO {

    @Override
    public int obtenerIdFacturaPorNumFactura(AvmovFacturaNdCab factura) {
        Connection cn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        int idFactura = 0;
        try {
            cn = Service.getConexion();
            String sql = "SELECT TOP 1 idFacturaCab "
                    + "FROM AVMOV_Factura_ND_CAB "
                    + "WHERE num_factura=? AND num_serie=? "
                    + "ORDER BY 1 DESC; ";

            ps = cn.prepareStatement(sql);
            ps.setString(1, factura.getNumFactura());
            ps.setString(2, factura.getNumSerie());

            rs = ps.executeQuery();

            if (rs.next()) {
                idFactura = rs.getInt("idFacturaCab");

            }
//            Service.cerrarConexion();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (cn.isClosed() == false) {
                    cn.close();
                }
            } catch (SQLException e) {
            }
        }

        return idFactura;
    }

    @Override
    public AvmovFacturaNdCab obtenerFacturaPorIdFactura(AvmovFacturaNdCab f) {
        Connection cn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        AvmovFacturaNdCab factura = null;
        try {
            cn = Service.getConexion();
            String sql = "SELECT AVMOV_Factura_ND_CAB.idFacturaCab, "
                    + "AVMOV_Factura_ND_CAB.num_movimiento, "
                    + "AVMOV_Factura_ND_CAB.idNotaDespachoCab, "
                    + "AVMOV_Factura_ND_CAB.ruc_companyia, "
                    + "AVMOV_Factura_ND_CAB.cod_establecimiento, "
                    + "AVMOV_Factura_ND_CAB.cod_centroc, "
                    + "AVMOV_Factura_ND_CAB.cod_Area, "
                    + "AVMOV_Factura_ND_CAB.cod_almacen, "
                    + "AVMOV_Factura_ND_CAB.num_anyio, "
                    + "AVMOV_Factura_ND_CAB.num_factura, "
                    + "AVMOV_Factura_ND_CAB.fec_factura, "
                    + "AVMOV_Factura_ND_CAB.num_nota_despacho, "
                    + "AVMOV_Factura_ND_CAB.cod_persona, "
                    + "AVMOV_Factura_ND_CAB.num_guia_remision, "
                    + "AVMOV_Factura_ND_CAB.cod_moneda, "
                    + "AVMOV_Factura_ND_CAB.val_dolar, "
                    + "AVMOV_Factura_ND_CAB.flg_tipo_factura, "
                    + "AVMOV_Factura_ND_CAB.flg_moneda_diferente, "
                    + "AVMOV_Factura_ND_CAB.val_IGV, "
                    + "AVMOV_Factura_ND_CAB.num_importe_subtotal, "
                    + "AVMOV_Factura_ND_CAB.num_importe_IGV, "
                    + "AVMOV_Factura_ND_CAB.num_importe_Total, "
                    + "AVMOV_Factura_ND_CAB.des_importe_total, "
                    + "AVMOV_Factura_ND_CAB.flg_prioridad, "
                    + "AVMOV_Factura_ND_CAB.flg_estado, "
                    + "AVMOV_Factura_ND_CAB.fec_creacion, "
                    + "AVMOV_Factura_ND_CAB.hor_creacion, "
                    + "AVMOV_Factura_ND_CAB.cod_usuario_creacion, "
                    + "AVMOV_Factura_ND_CAB.fec_actualizacion, "
                    + "AVMOV_Factura_ND_CAB.hor_actualizacion, "
                    + "AVMOV_Factura_ND_CAB.cod_usuario_actualizacion, "
                    + "agmae_persona.num_identificacion, "
                    + "agmae_persona.Nom_Razon_Social, "
                    + "agmae_persona.des_direccion_facturacion, "
                    + "agmae_distrito.des_distrito, "
                    + "agmae_companyias.des_companyia, "
                    + "AVMOV_Factura_ND_CAB.num_serie "
                    + "FROM (agmae_distrito RIGHT JOIN (AVMOV_Factura_ND_CAB "
                    + "INNER JOIN agmae_persona ON AVMOV_Factura_ND_CAB.cod_persona = agmae_persona.cod_persona) "
                    + "ON (agmae_distrito.cod_distrito = agmae_persona.cod_distrito) AND (agmae_distrito.cod_provincia = agmae_persona.cod_provincia) "
                    + "AND (agmae_distrito.cod_departamento = agmae_persona.cod_departamento)) "
                    + "INNER JOIN agmae_companyias ON AVMOV_Factura_ND_CAB.ruc_companyia = agmae_companyias.ruc_companyia "
                    + "WHERE (((AVMOV_Factura_ND_CAB.idFacturaCab)=[?]));";

            ps = cn.prepareStatement(sql);
            ps.setInt(1, f.getIdFacturaCab());

            rs = ps.executeQuery();

            factura = new AvmovFacturaNdCab();
            if (rs.next()) {
                factura.setIdFacturaCab(rs.getInt(1));
                factura.setNumMovimiento(rs.getString(2));
                factura.setIdNotaDespachoCab(rs.getInt(3));
                factura.setRucCompanyia(rs.getString(4));
                factura.setCodEstablecimiento(rs.getString(5));
                factura.setCodCentroc(rs.getInt(6));
                factura.setCodArea(rs.getInt(7));
                factura.setCodAlmacen(rs.getInt(8));
                factura.setNumAnyio(rs.getString(9));
                factura.setNumFactura(rs.getString(10));
                factura.setFecFactura(rs.getDate(11));
                factura.setNumNotaDespacho(rs.getString(12));
                factura.setCodPersona(rs.getInt(13));
                factura.setNumGuiaRemision(rs.getString(14));
                factura.setCodMoneda(rs.getString(15));
                factura.setValDolar(rs.getDouble(16));
                factura.setFlgTipoFactura(rs.getString(17));
                factura.setFlgMonedaDiferente(rs.getInt(18));
                factura.setValIgv(rs.getDouble(19));
                factura.setNumImporteSubtotal(rs.getDouble(20));
                factura.setNumImporteIgv(rs.getDouble(21));
                factura.setNumImporteTotal(rs.getDouble(22));
                factura.setDesImporteTotal(rs.getString(23));
                factura.setFlgPrioridad(rs.getString(24));
                factura.setFlgEstado(rs.getString(25));
                factura.setFecCreacion(rs.getString(26));
                factura.setHorCreacion(rs.getString(27));
                factura.setCodUsuarioCreacion(rs.getInt(28));
                factura.setFecActualizacion(rs.getString(29));
                factura.setHorActualizacion(rs.getString(30));
                factura.setCodUsuarioActualizacion(rs.getInt(31));
                factura.setZrucPersona(rs.getString(32));
                factura.setZnomPersona(rs.getString(33));
                factura.setZdirPersona(rs.getString(34));
                factura.setZdisPersona(rs.getString(35));
                factura.setZnomCompanya(rs.getString(36));
                factura.setNumSerie(rs.getString(37));

            }
//            Service.cerrarConexion();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (cn.isClosed() == false) {
                    cn.close();
                }
            } catch (SQLException e) {
            }
        }

        return factura;
    }

    @Override
    public AvmovFacturaNdCab obtenerFacturaPorND(AvmovMovNotaDespachoCab notaDespacho) {
        Connection cn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        AvmovFacturaNdCab factura = null;
        try {
            cn = Service.getConexion();
            String sql = "SELECT AVMOV_MovNotaDespachoCab.id_MovValeCab AS idMov, "
                    + "AVMOV_MovNotaDespachoCab.num_vale AS numND, AVMOV_MovNotaDespachoCab.ruc_companyia AS rucCompanya, agmae_companyias.des_companyia AS nomCompanya, AVMOV_MovNotaDespachoCab.cod_establecimiento AS codEstablecimiento, AVMOV_MovNotaDespachoCab.cod_centroc AS codCentro, AVMOV_MovNotaDespachoCab.cod_area AS codArea, AVMOV_MovNotaDespachoCab.cod_almacen AS codAlmacen, AVMOV_MovNotaDespachoCab.cod_persona AS codPersona, agmae_persona.num_identificacion AS rucPersona, agmae_persona.Nom_Razon_Social AS nomPersona, agmae_persona.des_direccion_facturacion AS dirPersona, agmae_distrito.des_distrito AS disPersona, (SELECT val_moneda_a FROM AVMOV_Conversor_Moneda WHERE (cod_moneda_de='2' AND cod_moneda_a='1' )) AS valDolar, (SELECT TOP 1 ((Agmae_IGV.IGV)) FROM Agmae_IGV ORDER BY Agmae_IGV.cod_documento DESC) AS Igv, (IIf(Sum(IIf(Z_Producto_Precio.cod_moneda='1',1,0))>0 And Sum(IIf(Z_Producto_Precio.cod_moneda='2',1,0))>0,1,0)) AS flgMonedaDiferente, Sum(IIf(Z_Producto_Precio.cod_moneda='1',Z_Producto_Precio.Precio_Producto,Z_Producto_Precio.Precio_Producto*(SELECT val_moneda_a FROM AVMOV_Conversor_Moneda WHERE (cod_moneda_de='2' AND cod_moneda_a='1' )))*AVMOV_MovNotaDespachoDet.ctd_Movimiento) AS valSubTotalSol, Sum(IIf(Z_Producto_Precio.cod_moneda='1',Z_Producto_Precio.Precio_Producto,Z_Producto_Precio.Precio_Producto*(SELECT val_moneda_a FROM AVMOV_Conversor_Moneda WHERE (cod_moneda_de='2' AND cod_moneda_a='1' )))*AVMOV_MovNotaDespachoDet.ctd_Movimiento*(Igv/100)) AS valIgvSol, Sum(IIf(Z_Producto_Precio.cod_moneda='1',Z_Producto_Precio.Precio_Producto,Z_Producto_Precio.Precio_Producto*(SELECT val_moneda_a FROM AVMOV_Conversor_Moneda WHERE (cod_moneda_de='2' AND cod_moneda_a='1' )))*AVMOV_MovNotaDespachoDet.ctd_Movimiento*((Igv/100)+1)) AS valTotalSol, AVMOV_MovNotaDespachoDet.flg_estado_factura\n"
                    + "FROM ((((AVMOV_MovNotaDespachoCab INNER JOIN AVMOV_MovNotaDespachoDet ON AVMOV_MovNotaDespachoCab.id_MovValeCab = AVMOV_MovNotaDespachoDet.id_MovValeCab) LEFT JOIN agmae_companyias ON AVMOV_MovNotaDespachoCab.ruc_companyia = agmae_companyias.ruc_companyia) LEFT JOIN agmae_persona ON AVMOV_MovNotaDespachoCab.cod_persona = agmae_persona.cod_persona) LEFT JOIN agmae_distrito ON (agmae_persona.cod_departamento_facturacion = agmae_distrito.cod_departamento) AND (agmae_persona.cod_provincia_facturacion = agmae_distrito.cod_provincia) AND (agmae_persona.cod_distrito_facturacion = agmae_distrito.cod_distrito)) LEFT JOIN Z_Producto_Precio ON AVMOV_MovNotaDespachoDet.cod_producto = Z_Producto_Precio.Cod_Producto\n"
                    + "GROUP BY AVMOV_MovNotaDespachoCab.id_MovValeCab, AVMOV_MovNotaDespachoCab.num_vale, AVMOV_MovNotaDespachoCab.ruc_companyia, agmae_companyias.des_companyia, AVMOV_MovNotaDespachoCab.cod_establecimiento, AVMOV_MovNotaDespachoCab.cod_centroc, AVMOV_MovNotaDespachoCab.cod_area, AVMOV_MovNotaDespachoCab.cod_almacen, AVMOV_MovNotaDespachoCab.cod_persona, agmae_persona.num_identificacion, agmae_persona.cod_identificacion, agmae_persona.Nom_Razon_Social, agmae_persona.des_direccion_facturacion, agmae_distrito.des_distrito, AVMOV_MovNotaDespachoDet.flg_estado_factura\n"
                    + "HAVING (((AVMOV_MovNotaDespachoCab.id_MovValeCab)=[?]) AND ((AVMOV_MovNotaDespachoCab.ruc_companyia)=[?]) AND ((AVMOV_MovNotaDespachoDet.flg_estado_factura)=0));";

            ps = cn.prepareStatement(sql);
            ps.setInt(1, notaDespacho.getIdMovValeCab());
            ps.setString(2, notaDespacho.getRucCompanyia());
            rs = ps.executeQuery();
            factura = new AvmovFacturaNdCab();
            if (rs.next()) {
                factura.setIdNotaDespachoCab(rs.getInt("idMov"));
                factura.setNumNotaDespacho(rs.getString("numND"));
                factura.setRucCompanyia(rs.getString("rucCompanya"));
                factura.setZnomCompanya(rs.getString("nomCompanya"));
                factura.setCodEstablecimiento(rs.getString("codEstablecimiento"));
                factura.setCodCentroc(rs.getInt("codCentro"));
                factura.setCodArea(rs.getInt("codArea"));
                factura.setCodAlmacen(rs.getInt("codAlmacen"));
                factura.setCodPersona(rs.getInt("codPersona"));
                factura.setZrucPersona(rs.getString("rucPersona"));
                factura.setZnomPersona(rs.getString("nomPersona"));
                factura.setZdirPersona(rs.getString("dirPersona"));
                factura.setZdisPersona(rs.getString("disPersona"));
                factura.setValDolar(rs.getDouble("valDolar"));
                factura.setValIgv(rs.getDouble("Igv"));
                factura.setFlgMonedaDiferente(rs.getInt("flgMonedaDiferente"));
                factura.setZvalorSubTotalSol(Math.round(rs.getDouble("valSubTotalSol") * 100.0) / 100.0);
                factura.setZvalorIgvSol(Math.round(rs.getDouble("valIgvSol") * 100.0) / 100.0);
                factura.setZvalorTotalSol(Math.round(rs.getDouble("valTotalSol") * 100.0) / 100.0);

            }
//            Service.cerrarConexion();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (cn.isClosed() == false) {
                    cn.close();
                }
            } catch (SQLException e) {
            }
        }

        return factura;
    }

    @Override
    public List<AvmovFacturaNdCab> listarFacturasPorFecha(Date feDesde, Date feHasta) {
        Connection cn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        boolean existe = false;
        AvmovFacturaNdCab factura = null;
        List<AvmovFacturaNdCab> lista = new ArrayList<>();

        try {
            cn = Service.getConexion();
            String sql = "SELECT AVMOV_Factura_ND_CAB.idFacturaCab AS idFac, AVMOV_Factura_ND_CAB.num_serie AS numSerie, AVMOV_Factura_ND_CAB.num_movimiento AS numMov, AVMOV_Factura_ND_CAB.idNotaDespachoCab AS idMovAlm, AVMOV_Factura_ND_CAB.ruc_companyia AS rucCompanya, agmae_companyias.des_companyia AS nomCompanya, AVMOV_Factura_ND_CAB.cod_establecimiento AS codEstablecimieto, AVMOV_Factura_ND_CAB.cod_centroc AS codCentro, AVMOV_Factura_ND_CAB.cod_Area AS codArea, AVMOV_Factura_ND_CAB.cod_almacen AS codAlmacen, AVMOV_Factura_ND_CAB.num_anyio AS numAnyo, AVMOV_Factura_ND_CAB.num_factura AS numFac, AVMOV_Factura_ND_CAB.fec_factura AS fecFac, AVMOV_Factura_ND_CAB.num_nota_despacho AS numND, AVMOV_Factura_ND_CAB.cod_persona AS codPersona, agmae_persona.num_identificacion AS rucPersona, agmae_persona.Nom_Razon_Social AS nomPersona, AVMOV_Factura_ND_CAB.num_guia_remision AS numGR, AVMOV_Factura_ND_CAB.cod_moneda AS codMoneda, agmae_moneda.des_moneda AS nomMoneda, agmae_moneda.abr_moneda AS abrMoneda, AVMOV_Factura_ND_CAB.val_dolar AS valDolar, AVMOV_Factura_ND_CAB.flg_tipo_factura AS flgTipoFac, AVMOV_Factura_ND_CAB.flg_moneda_diferente AS flgMonedaDiferente, AVMOV_Factura_ND_CAB.val_IGV AS igv, AVMOV_Factura_ND_CAB.num_importe_subtotal AS impSubTotal, AVMOV_Factura_ND_CAB.num_importe_IGV AS impIgv, AVMOV_Factura_ND_CAB.num_importe_Total AS impTotal, AVMOV_Factura_ND_CAB.des_importe_total AS desImpTotal, AVMOV_Factura_ND_CAB.flg_prioridad AS flgPrioridad, IIf(AVMOV_Factura_ND_CAB.flg_prioridad='1','NORMAL',IIf(AVMOV_Factura_ND_CAB.flg_prioridad='0','BAJA','ALTA')) AS nomPrioridad, AVMOV_Factura_ND_CAB.flg_estado AS flgEstado, AVMOV_Factura_ND_CAB.fec_creacion AS fecCreacion, AVMOV_Factura_ND_CAB.hor_creacion AS horCreacion, AVMOV_Factura_ND_CAB.cod_usuario_creacion AS usuCreacion, AVMOV_Factura_ND_CAB.fec_actualizacion AS fecActualizacion, AVMOV_Factura_ND_CAB.hor_actualizacion AS horActualizacion, AVMOV_Factura_ND_CAB.cod_usuario_actualizacion AS usuActualizacion, AGMAE_ESTADO_TIPO_DOCUMENTO.des_estado AS nomEstado, AGMAE_ESTADO.des_estado AS estImprimir, AGMAE_ESTADO_1.des_estado AS estEditar, AGMAE_ESTADO_2.des_estado AS estAnular, "
                    + "IIF((SELECT Count(fd.idFacturaCab) AS Cantidad "
                    + "FROM AVMOV_Factura_ND_DET AS fd "
                    + "WHERE (((fd.flg_estado_guia_remision)=0) AND ((fd.idFacturaCab)=AVMOV_Factura_ND_CAB.idFacturaCab )))>0 "
                    + ",'false','true') AS estGuiaRemision  "
                    + "FROM (((AGMAE_ESTADO_TIPO_DOCUMENTO RIGHT JOIN (((AVMOV_Factura_ND_CAB LEFT JOIN agmae_moneda ON AVMOV_Factura_ND_CAB.cod_moneda = agmae_moneda.cod_moneda) LEFT JOIN agmae_companyias ON AVMOV_Factura_ND_CAB.ruc_companyia = agmae_companyias.ruc_companyia) LEFT JOIN agmae_persona ON AVMOV_Factura_ND_CAB.cod_persona = agmae_persona.cod_persona) ON (AGMAE_ESTADO_TIPO_DOCUMENTO.cod_documento = AVMOV_Factura_ND_CAB.cod_tipo_documento) AND (AGMAE_ESTADO_TIPO_DOCUMENTO.cod_estado = AVMOV_Factura_ND_CAB.flg_estado)) LEFT JOIN AGMAE_ESTADO ON AGMAE_ESTADO_TIPO_DOCUMENTO.flg_button_print = AGMAE_ESTADO.cod_estado) LEFT JOIN AGMAE_ESTADO AS AGMAE_ESTADO_1 ON AGMAE_ESTADO_TIPO_DOCUMENTO.flg_button_edit = AGMAE_ESTADO_1.cod_estado) LEFT JOIN AGMAE_ESTADO AS AGMAE_ESTADO_2 ON AGMAE_ESTADO_TIPO_DOCUMENTO.flg_button_del = AGMAE_ESTADO_2.cod_estado\n"
                    + "WHERE (((AVMOV_Factura_ND_CAB.fec_factura)>=[?] And (AVMOV_Factura_ND_CAB.fec_factura)<=[?]))\n"
                    + "ORDER BY AVMOV_Factura_ND_CAB.fec_factura DESC , AVMOV_Factura_ND_CAB.hor_creacion DESC;";

            ps = cn.prepareStatement(sql);
            ps.setDate(1, convertJavaDateToSqlDate(feDesde));
            ps.setDate(2, convertJavaDateToSqlDate(feHasta));

            rs = ps.executeQuery();

            while (rs.next()) {
                existe = true;
                factura = new AvmovFacturaNdCab();

                factura.setIdFacturaCab(rs.getInt("idFac"));
                factura.setNumSerie(rs.getString("numSerie"));
                factura.setNumMovimiento(rs.getString("numMov"));
                factura.setIdNotaDespachoCab(rs.getInt("idMovAlm"));
                factura.setRucCompanyia(rs.getString("rucCompanya"));
                factura.setZnomCompanya(rs.getString("nomCompanya"));
                factura.setCodEstablecimiento(rs.getString("codEstablecimieto"));
                factura.setCodCentroc(rs.getInt("codCentro"));
                factura.setCodArea(rs.getInt("codArea"));
                factura.setCodAlmacen(rs.getInt("codAlmacen"));
                factura.setNumAnyio(rs.getString("numAnyo"));
                factura.setNumFactura(rs.getString("numFac"));
                factura.setFecFactura(rs.getDate("fecFac"));
                factura.setNumNotaDespacho(rs.getString("numND"));
                factura.setCodPersona(rs.getInt("codPersona"));
                factura.setZrucPersona(rs.getString("rucPersona"));
                factura.setZnomPersona(rs.getString("nomPersona"));
                factura.setNumGuiaRemision(rs.getString("numGR"));
                factura.setCodMoneda(rs.getString("codMoneda"));
                factura.setZnomMoneda(rs.getString("nomMoneda"));
                factura.setZabrMoneda(rs.getString("abrMoneda"));
                factura.setValDolar(rs.getDouble("valDolar"));
                factura.setFlgTipoFactura(rs.getString("flgTipoFac"));
                factura.setFlgMonedaDiferente(rs.getInt("flgMonedaDiferente"));
                factura.setValIgv(rs.getDouble("igv"));
                factura.setNumImporteSubtotal(rs.getDouble("impSubTotal"));
                factura.setNumImporteIgv(rs.getDouble("impIgv"));
                factura.setNumImporteTotal(rs.getDouble("impTotal"));
                factura.setDesImporteTotal(rs.getString("desImpTotal"));
                factura.setFlgPrioridad(rs.getString("flgPrioridad"));
                factura.setZnomPrioridad(rs.getString("nomPrioridad"));
                factura.setFlgEstado(rs.getString("flgEstado"));
                factura.setZnomEstado(rs.getString("nomEstado"));
                factura.setFecCreacion(rs.getString("fecCreacion"));
                factura.setHorCreacion(rs.getString("horCreacion"));
                factura.setCodUsuarioCreacion(rs.getInt("usuCreacion"));
                factura.setFecActualizacion(rs.getString("fecActualizacion"));
                factura.setHorActualizacion(rs.getString("horActualizacion"));
                factura.setCodUsuarioActualizacion(rs.getInt("usuActualizacion"));
                factura.setZestadoImprimir(rs.getString("estImprimir"));
                factura.setZestadoEditar(rs.getString("estEditar"));
                factura.setZestadoAnular(rs.getString("estAnular"));
                factura.setZestadoGuiaRemision(rs.getString("estGuiaRemision"));
                lista.add(factura);
            }
            if (!existe) {
                factura = null;
                System.out.println("No se encontro datos");
            }
//              Service.cerrarConexion();

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (cn.isClosed() == false) {
                    cn.close();
                }
            } catch (SQLException e) {
            }
        }
        return lista;

    }

    @Override
    public void newFactura(AvmovFacturaNdCab factura) {
        Connection cn = null;
        PreparedStatement ps = null;
        String sql = "INSERT INTO AVMOV_Factura_ND_CAB ("
                + "`num_movimiento`, "
                + "`idNotaDespachoCab`, "
                + "`ruc_companyia`, "
                + "`cod_establecimiento`, "
                + "`cod_centroc`, "
                + "`cod_Area`, "
                + "`cod_almacen`, "
                + "`num_anyio`, "
                + "`num_factura`, "
                + "`fec_factura`, "
                + "`num_nota_despacho`, "
                + "`cod_persona`, "
                + "`num_guia_remision`, "
                + "`cod_moneda`, "
                + "`val_dolar`, "
                + "`flg_tipo_factura`, "
                + "`flg_moneda_diferente`, "
                + "`val_IGV`, "
                + "`num_importe_subtotal`, "
                + "`num_importe_IGV`, "
                + "`num_importe_Total`, "
                + "`des_importe_total`, "
                + "`flg_prioridad`, "
                + "`flg_estado`, "
                + "`fec_creacion`, "
                + "`hor_creacion`, "
                + "`cod_usuario_creacion`,"
                + "`num_serie`,"
                + "`cod_tipo_documento`) "
                + "VALUES("
                + "?,?,?,?,?,?,?,?,?,?,"
                + "?,?,?,?,?,?,?,?,?,?,"
                + "?,?,?,?,?,?,?,?,?); ";

        Date fechaActual = new Date();
        SimpleDateFormat formatoFecha = new SimpleDateFormat("dd/MM/yyyy");
        SimpleDateFormat formatoHora = new SimpleDateFormat("HH:mm:ss");

        factura.setFecCreacion(formatoFecha.format(fechaActual));
        factura.setHorCreacion(formatoHora.format(fechaActual));

        try {
            cn = Service.getConexion();
            ps = cn.prepareStatement(sql);
            ps.setString(1, factura.getNumMovimiento());
            ps.setInt(2, factura.getIdNotaDespachoCab());
            ps.setString(3, factura.getRucCompanyia());
            ps.setString(4, factura.getCodEstablecimiento());
            ps.setInt(5, factura.getCodCentroc());
            ps.setInt(6, factura.getCodArea());
            ps.setInt(7, factura.getCodAlmacen());
            ps.setString(8, factura.getNumAnyio());
            ps.setString(9, factura.getNumFactura());
            ps.setDate(10, convertJavaDateToSqlDate(factura.getFecFactura()));
            ps.setString(11, factura.getNumNotaDespacho());
            ps.setInt(12, factura.getCodPersona());
            ps.setString(13, factura.getNumGuiaRemision());
            ps.setString(14, factura.getCodMoneda());
            ps.setDouble(15, factura.getValDolar());
            ps.setString(16, factura.getFlgTipoFactura());
            ps.setInt(17, factura.getFlgMonedaDiferente());
            ps.setDouble(18, factura.getValIgv());
            ps.setDouble(19, factura.getNumImporteSubtotal());
            ps.setDouble(20, factura.getNumImporteIgv());
            ps.setDouble(21, factura.getNumImporteTotal());
            ps.setString(22, factura.getDesImporteTotal());
            ps.setString(23, factura.getFlgPrioridad());
            ps.setString(24, factura.getFlgEstado());
            ps.setString(25, factura.getFecCreacion());
            ps.setString(26, factura.getHorCreacion());
            ps.setInt(27, factura.getCodUsuarioCreacion());
            ps.setString(28, factura.getNumSerie());
            ps.setString(29, factura.getCodTipoDocumento());
            ps.executeUpdate();
//              Service.cerrarConexion();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (cn.isClosed() == false) {
                    cn.close();
                }
            } catch (SQLException e) {
            }
        }
        factura.setIdFacturaCab(obtenerIdFacturaPorNumFactura(factura));
        newRelFacND(factura);
    }

    @Override
    public void updateFactura(AvmovFacturaNdCab factura) {
        Connection cn = null;
        PreparedStatement ps = null;
        String sql = "UPDATE AVMOV_Factura_ND_CAB SET "
                + "num_movimiento=?, \n"
                + "idNotaDespachoCab=?, \n"
                + "ruc_companyia=?, \n"
                + "cod_establecimiento=?, \n"
                + "cod_centroc=?, \n"
                + "cod_Area=?, \n"
                + "cod_almacen=?, \n"
                + "num_anyio=?, \n"
                + "num_factura=?, \n"
                + "fec_factura=?, \n"
                + "num_nota_despacho=?, \n"
                + "cod_persona=?, \n"
                + "num_guia_remision=?, \n"
                + "cod_moneda=?, \n"
                + "val_dolar=?, \n"
                + "flg_tipo_factura=?, \n"
                + "flg_moneda_diferente=?, \n"
                + "val_IGV=?, \n"
                + "num_importe_subtotal=?, \n"
                + "num_importe_IGV=?, \n"
                + "num_importe_Total=?, \n"
                + "des_importe_total=?, \n"
                + "flg_prioridad=?, \n"
                + "flg_estado=?, \n"
                + "fec_actualizacion=?, \n"
                + "hor_actualizacion=?, \n"
                + "cod_usuario_actualizacion=?, "
                + "num_serie=? "
                + "WHERE idFacturaCab=?; ";

        Date fechaActual = new Date();
        SimpleDateFormat formatoFecha = new SimpleDateFormat("dd/MM/yyyy");
        SimpleDateFormat formatoHora = new SimpleDateFormat("HH:mm:ss");

        factura.setFecCreacion(formatoFecha.format(fechaActual));
        factura.setHorCreacion(formatoHora.format(fechaActual));

        try {
            cn = Service.getConexion();
            ps = cn.prepareStatement(sql);
            ps.setString(1, factura.getNumMovimiento());
            ps.setInt(2, factura.getIdNotaDespachoCab());
            ps.setString(3, factura.getRucCompanyia());
            ps.setString(4, factura.getCodEstablecimiento());
            ps.setInt(5, factura.getCodCentroc());
            ps.setInt(6, factura.getCodArea());
            ps.setInt(7, factura.getCodAlmacen());
            ps.setString(8, factura.getNumAnyio());
            ps.setString(9, factura.getNumFactura());
            ps.setDate(10, convertJavaDateToSqlDate(factura.getFecFactura()));
            ps.setString(11, factura.getNumNotaDespacho());
            ps.setInt(12, factura.getCodPersona());
            ps.setString(13, factura.getNumGuiaRemision());
            ps.setString(14, factura.getCodMoneda());
            ps.setDouble(15, factura.getValDolar());
            ps.setString(16, factura.getFlgTipoFactura());
            ps.setInt(17, factura.getFlgMonedaDiferente());
            ps.setDouble(18, factura.getValIgv());
            ps.setDouble(19, factura.getNumImporteSubtotal());
            ps.setDouble(20, factura.getNumImporteIgv());
            ps.setDouble(21, factura.getNumImporteTotal());
            ps.setString(22, factura.getDesImporteTotal());
            ps.setString(23, factura.getFlgPrioridad());
            ps.setString(24, factura.getFlgEstado());
            ps.setString(25, factura.getFecActualizacion());
            ps.setString(26, factura.getHorActualizacion());
            ps.setInt(27, factura.getCodUsuarioActualizacion());
            ps.setString(28, factura.getNumSerie());
            ps.setInt(29, factura.getIdFacturaCab());

            ps.executeUpdate();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (cn.isClosed() == false) {
                    cn.close();
                }
            } catch (SQLException e) {
            }
        }
    }

    @Override
    public void deleteFactura(AvmovFacturaNdCab factura) {
        Connection cn = null;
        PreparedStatement ps = null;
        String sql = "DELETE FROM AVMOV_Factura_ND_CAB "
                + "WHERE idFacturaCab=?; ";
        try {
            cn = Service.getConexion();
            ps = cn.prepareStatement(sql);
            ps.setInt(1, factura.getIdFacturaCab());

            ps.executeUpdate();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (cn.isClosed() == false) {
                    cn.close();
                }
            } catch (SQLException e) {
            }
        }
    }

    public java.sql.Date convertJavaDateToSqlDate(java.util.Date date) {
        return new java.sql.Date(date.getTime());
    }

    public void newRelFacND(AvmovFacturaNdCab factura) {
        Connection cn = null;
        PreparedStatement ps = null;
        String sql = "INSERT INTO AVMOV_REL_ND_FACT ("
                + "`id_MovValeCab`,"
                + "`idFacturaCab`,"
                + "`ruc_companyia`, "
                + "`num_factura`, "
                + "`cod_establecimiento`, "
                + "`cod_centroc`, "
                + "`cod_area`, "
                + "`cod_almacen`, "
                + "`Cod_TipoProducto`, "
                + "`num_movimiento`, "
                + "`fec_creacion`, "
                + "`hor_creacion`, "
                + " `cod_usuario_creacion`"
                + ") "
                + "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?); ";

        Date fechaActual = new Date();
        SimpleDateFormat formatoFecha = new SimpleDateFormat("dd/MM/yyyy");
        SimpleDateFormat formatoHora = new SimpleDateFormat("HH:mm:ss");
        factura.setFecCreacion(formatoFecha.format(fechaActual));
        factura.setHorCreacion(formatoHora.format(fechaActual));
        try {
            cn = Service.getConexion();
            ps = cn.prepareStatement(sql);
            ps.setInt(1, factura.getIdFacturaCab());
            ps.setInt(2, factura.getIdNotaDespachoCab());
            ps.setString(3, factura.getRucCompanyia());
            ps.setString(4, factura.getNumFactura());
            ps.setString(5, factura.getCodEstablecimiento());
            ps.setInt(6, factura.getCodCentroc());
            ps.setInt(7, factura.getCodArea());
            ps.setInt(8, factura.getCodAlmacen());
            ps.setString(9, "MT");
            ps.setString(10, factura.getNumMovimiento());
            ps.setString(11, factura.getFecCreacion());
            ps.setString(12, factura.getHorCreacion());
            ps.setInt(13, factura.getCodUsuarioCreacion());

            ps.executeUpdate();
//              Service.cerrarConexion();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (cn.isClosed() == false) {
                    cn.close();
                }
            } catch (SQLException e) {
            }
        }
    }

    @Override
    public String generarNroFactura(AvmovFacturaNdCab factura) {
        String numDocumento = "";
        String cesDocumento = "";
        numDocumento = obtenerNroFactura(factura);
        if (numDocumento.equals("")) {
            newNroSerieFactura(factura);
        }
        newNroFactura(factura);

        Connection cn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;

        try {
            cn = Service.getConexion();
            String sql = "SELECT Agmae_numerador_documentos.num_documento, "
                    + "Agmae_numerador_documentos.ces_documento "
                    + "FROM Agmae_numerador_documentos "
                    + "WHERE (((Agmae_numerador_documentos.ruc_companyia)=[?]) "
                    + "AND ((Agmae_numerador_documentos.cod_documento)='01') "
                    + "AND ((Agmae_numerador_documentos.num_serie_documento)=[?]));";

            ps = cn.prepareStatement(sql);
            ps.setString(1, factura.getRucCompanyia());
            ps.setString(2, factura.getNumSerie());
            rs = ps.executeQuery();

            if (rs.next()) {
                numDocumento = String.valueOf(rs.getInt(1));
                cesDocumento = rs.getString(2);
                for (int i = 1; numDocumento.length() <= 4; i++) {
                    numDocumento = "0" + numDocumento;
                }
                numDocumento = cesDocumento + numDocumento;
            }
//            Service.cerrarConexion();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (cn.isClosed() == false) {
                    cn.close();
                }
            } catch (SQLException e) {
            }
        }

        return numDocumento;
    }

    public String obtenerNroFactura(AvmovFacturaNdCab factura) {
        Connection cn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        String numDocumento = "";
        try {
            cn = Service.getConexion();
            String sql = "SELECT Agmae_numerador_documentos.num_documento "
                    + "FROM Agmae_numerador_documentos "
                    + "WHERE (((Agmae_numerador_documentos.ruc_companyia)=[?]) "
                    + "AND ((Agmae_numerador_documentos.cod_documento)='01') "
                    + "AND ((Agmae_numerador_documentos.num_serie_documento)=[?]));";

            ps = cn.prepareStatement(sql);
            ps.setString(1, factura.getRucCompanyia());
            ps.setString(2, factura.getNumSerie());
            rs = ps.executeQuery();

            if (rs.next()) {
                numDocumento = String.valueOf(rs.getInt(1));

            }
//            Service.cerrarConexion();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (cn.isClosed() == false) {
                    cn.close();
                }
            } catch (SQLException e) {
            }
        }

        return numDocumento;
    }

    public void newNroFactura(AvmovFacturaNdCab factura) {
        Connection cn = null;
        PreparedStatement ps = null;
        try {
            cn = Service.getConexion();
            String sql = "UPDATE Agmae_numerador_documentos SET"
                    + " num_documento=num_documento+1 "
                    + "WHERE (((ruc_companyia)=[?]) "
                    + "AND ((cod_documento)='01') "
                    + "AND ((num_serie_documento)=[?]));";

            ps = cn.prepareStatement(sql);
            ps.setString(1, factura.getRucCompanyia());
            ps.setString(2, factura.getNumSerie());
            ps.executeUpdate();

//            Service.cerrarConexion();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (cn.isClosed() == false) {
                    cn.close();
                }
            } catch (SQLException e) {
            }
        }
    }

    public void newNroSerieFactura(AvmovFacturaNdCab factura) {
        Connection cn = null;
        PreparedStatement ps = null;

        try {
            cn = Service.getConexion();
            String sql = "INSERT INTO Agmae_numerador_documentos(num_documento,ces_documento, "
                    + "cod_documento,ruc_companyia,num_serie_documento) "
                    + "VALUES(0,'F','01',?,?);";

            ps = cn.prepareStatement(sql);
            ps.setString(1, factura.getRucCompanyia());
            ps.setString(2, factura.getNumSerie());
            ps.executeUpdate();

//            Service.cerrarConexion();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (cn.isClosed() == false) {
                    cn.close();
                }
            } catch (SQLException e) {
            }
        }

    }

}
