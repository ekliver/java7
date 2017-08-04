/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package sys.imp;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import sys.dao.AlmacenDAO;
import sys.model.AgmaeUsuario;
import sys.model.AimarAlmacen;
import sys.model.AvmovMovNotaDespachoCab;
import sys.util.Service;

/**
 *
 * @author cocotin
 */
public class AlmacenDAOImp implements AlmacenDAO {

    @Override
    public List<AimarAlmacen> listarAlmacenes(AgmaeUsuario usuario) {
        Connection cn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        boolean existe = false;
        AimarAlmacen almacen = null;
        List<AimarAlmacen> lista = new ArrayList<>();
        try {
            cn = Service.getConexion();
            String sql = "SELECT AIMAR_Almacen.Cod_Ai_Almacen AS Cod_Ai_Almacen, "
                    + "AIMAR_Almacen.Nom_Ai_Almacen AS Nom_Ai_Almacen, "
                    + "AIMAR_Almacen.UltInv_Ai_Almacen AS UltInv_Ai_Almacen "
                    + "FROM   AIMAR_Almacen INNER JOIN AIMAR_EstabXAlmacen "
                    + "ON (AIMAR_Almacen.Cod_Ai_Almacen = AIMAR_EstabXAlmacen.Cod_Ai_Almacen) "
                    + "AND (AIMAR_Almacen.ruc_companyia = AIMAR_EstabXAlmacen.ruc_companyia) "
                    + "WHERE  AIMAR_EstabXAlmacen.ruc_companyia=? "
                    + "AND AIMAR_EstabXAlmacen.COD_ESTABLECIMIENTO=? "
                    + "AND AIMAR_EstabXAlmacen.COD_CENTROC=? ;";

            ps = cn.prepareStatement(sql);
            ps.setString(1, usuario.getRucCompanyia());
            ps.setString(2, usuario.getCodEstablecimiento());
            ps.setInt(3, usuario.getCodCentroc());
                    rs = ps.executeQuery();
            while (rs.next()) {
                existe = true;
                almacen = new AimarAlmacen();
          
                almacen.setCodAiAlmacen(rs.getInt("Cod_Ai_Almacen"));
                almacen.setNomAiAlmacen(rs.getString("Nom_Ai_Almacen"));
                almacen.setUltInvAiAlmacen(rs.getDate("UltInv_Ai_Almacen"));
          
                lista.add(almacen);
            }
            if (!existe) {
                almacen = null;
                System.out.println("No se encontro datos");
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
        return lista;
    }

    @Override
    public List<AimarAlmacen> listarAlmacenes(String rucCompanya) {
        Connection cn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        boolean existe = false;
        AimarAlmacen almacen = null;
        List<AimarAlmacen> lista = new ArrayList<>();
        try {
            cn = Service.getConexion();
            String sql = "SELECT ruc_companyia, Cod_Ai_Almacen, Nom_Ai_Almacen, "
                    + "Dir_Ai_Almacen, Local_Defecto_Ai_Almacen, flg_AbrevActivoFijo, "
                    + "num_orden, ValIng01_Ai_Almacen, ValIng02_Ai_Almacen, "
                    + "ValIng03_Ai_Almacen, ValIng04_Ai_Almacen, ValIng05_Ai_Almacen, "
                    + "ValIng06_Ai_Almacen, ValIng07_Ai_Almacen, ValIng08_Ai_Almacen, "
                    + "ValIng09_Ai_Almacen, ValIng10_Ai_Almacen, ValIng11_Ai_Almacen, "
                    + "ValIng12_Ai_Almacen, ValSal01_Ai_Almacen, ValSal02_Ai_Almacen, "
                    + "ValSal03_Ai_Almacen, ValSal04_Ai_Almacen, ValSal05_Ai_Almacen, "
                    + "ValSal06_Ai_Almacen, ValSal07_Ai_Almacen, ValSal08_Ai_Almacen, "
                    + "ValSal09_Ai_Almacen, ValSal10_Ai_Almacen, ValSal11_Ai_Almacen, "
                    + "ValSal12_Ai_Almacen, UltInv_Ai_Almacen, UltSal_Ai_Almacen, "
                    + "Centro_Ai_Almacen, Fec_Cie01_Ai_Almacen, Fec_Cie02_Ai_Almacen, "
                    + "Fec_Cie03_Ai_Almacen, Fec_Cie04_Ai_Almacen, Fec_Cie05_Ai_Almacen, "
                    + "Fec_Cie06_Ai_Almacen, Fec_Cie07_Ai_Almacen, Fec_Cie08_Ai_Almacen, "
                    + "Fec_Cie09_Ai_Almacen, Fec_Cie10_Ai_Almacen, Fec_Cie11_Ai_Almacen, "
                    + "Fec_Cie12_Ai_Almacen, Cod_Tip_Prod, TipoCosto_Ai_Almacen, "
                    + "Cod_Ai_Estado, fec_creacion, hor_creacion, cod_usuario_creacion, "
                    + "fec_actualizacion, hor_actualizacion, cod_usuario_actualizacion, "
                    + "flg_sinregistro "
                    + "FROM AIMAR_Almacen "
                    + "WHERE ruc_companyia=?;";

            ps = cn.prepareStatement(sql);
            ps.setString(1, rucCompanya);
            rs = ps.executeQuery();
            while (rs.next()) {
                existe = true;
                almacen = new AimarAlmacen();
                almacen.setRucCompanyia(rs.getString("ruc_companyia"));
                almacen.setCodAiAlmacen(rs.getInt("Cod_Ai_Almacen"));
                almacen.setNomAiAlmacen(rs.getString("Nom_Ai_Almacen"));
                almacen.setDirAiAlmacen(rs.getString("Dir_Ai_Almacen"));
                almacen.setLocalDefectoAiAlmacen(rs.getString("Local_Defecto_Ai_Almacen"));
                almacen.setFlgAbrevActivoFijo(rs.getString("flg_AbrevActivoFijo"));
                almacen.setNumOrden(rs.getInt("num_orden"));
                almacen.setValIng01AiAlmacen(rs.getString("ValIng01_Ai_Almacen"));
                almacen.setValIng02AiAlmacen(rs.getString("ValIng02_Ai_Almacen"));
                almacen.setValIng03AiAlmacen(rs.getString("ValIng03_Ai_Almacen"));
                almacen.setValIng04AiAlmacen(rs.getString("ValIng04_Ai_Almacen"));
                almacen.setValIng05AiAlmacen(rs.getString("ValIng05_Ai_Almacen"));
                almacen.setValIng06AiAlmacen(rs.getString("ValIng06_Ai_Almacen"));
                almacen.setValIng07AiAlmacen(rs.getString("ValIng07_Ai_Almacen"));
                almacen.setValIng08AiAlmacen(rs.getString("ValIng08_Ai_Almacen"));
                almacen.setValIng09AiAlmacen(rs.getString("ValIng09_Ai_Almacen"));
                almacen.setValIng10AiAlmacen(rs.getString("ValIng10_Ai_Almacen"));
                almacen.setValIng11AiAlmacen(rs.getString("ValIng11_Ai_Almacen"));
                almacen.setValIng12AiAlmacen(rs.getString("ValIng12_Ai_Almacen"));
                almacen.setValSal01AiAlmacen(rs.getString("ValSal01_Ai_Almacen"));
                almacen.setValSal02AiAlmacen(rs.getString("ValSal02_Ai_Almacen"));
                almacen.setValSal03AiAlmacen(rs.getString("ValSal03_Ai_Almacen"));
                almacen.setValSal04AiAlmacen(rs.getString("ValSal04_Ai_Almacen"));
                almacen.setValSal05AiAlmacen(rs.getString("ValSal05_Ai_Almacen"));
                almacen.setValSal06AiAlmacen(rs.getString("ValSal06_Ai_Almacen"));
                almacen.setValSal07AiAlmacen(rs.getString("ValSal07_Ai_Almacen"));
                almacen.setValSal08AiAlmacen(rs.getString("ValSal08_Ai_Almacen"));
                almacen.setValSal09AiAlmacen(rs.getString("ValSal09_Ai_Almacen"));
                almacen.setValSal10AiAlmacen(rs.getString("ValSal10_Ai_Almacen"));
                almacen.setValSal11AiAlmacen(rs.getString("ValSal11_Ai_Almacen"));
                almacen.setValSal12AiAlmacen(rs.getString("ValSal12_Ai_Almacen"));
                almacen.setUltInvAiAlmacen(rs.getDate("UltInv_Ai_Almacen"));
                almacen.setUltSalAiAlmacen(rs.getDate("UltSal_Ai_Almacen"));
                almacen.setCentroAiAlmacen(rs.getString("Centro_Ai_Almacen"));
                almacen.setFecCie01AiAlmacen(rs.getDate("Fec_Cie01_Ai_Almacen"));
                almacen.setFecCie02AiAlmacen(rs.getDate("Fec_Cie02_Ai_Almacen"));
                almacen.setFecCie03AiAlmacen(rs.getDate("Fec_Cie03_Ai_Almacen"));
                almacen.setFecCie04AiAlmacen(rs.getDate("Fec_Cie04_Ai_Almacen"));
                almacen.setFecCie05AiAlmacen(rs.getDate("Fec_Cie05_Ai_Almacen"));
                almacen.setFecCie06AiAlmacen(rs.getDate("Fec_Cie06_Ai_Almacen"));
                almacen.setFecCie07AiAlmacen(rs.getDate("Fec_Cie07_Ai_Almacen"));
                almacen.setFecCie08AiAlmacen(rs.getDate("Fec_Cie08_Ai_Almacen"));
                almacen.setFecCie09AiAlmacen(rs.getDate("Fec_Cie09_Ai_Almacen"));
                almacen.setFecCie10AiAlmacen(rs.getDate("Fec_Cie10_Ai_Almacen"));
                almacen.setFecCie11AiAlmacen(rs.getDate("Fec_Cie11_Ai_Almacen"));
                almacen.setFecCie12AiAlmacen(rs.getDate("Fec_Cie12_Ai_Almacen"));
                almacen.setCodTipProd(rs.getInt("Cod_Tip_Prod"));
                almacen.setTipoCostoAiAlmacen(rs.getString("TipoCosto_Ai_Almacen"));
                almacen.setCodAiEstado(rs.getString("Cod_Ai_Estado"));
                almacen.setFecCreacion(rs.getString("fec_creacion"));
                almacen.setHorCreacion(rs.getString("hor_creacion"));
                almacen.setCodUsuarioCreacion(rs.getInt("cod_usuario_creacion"));
                almacen.setFecActualizacion(rs.getString("fec_actualizacion"));
                almacen.setHorActualizacion(rs.getString("hor_actualizacion"));
                almacen.setCodUsuarioActualizacion(rs.getInt("cod_usuario_actualizacion"));
                almacen.setFlgSinregistro(rs.getString("flg_sinregistro"));

                lista.add(almacen);
            }
            if (!existe) {
                almacen = null;
                System.out.println("No se encontro datos");
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
        return lista;
    }

    @Override
    public AimarAlmacen consultarObjAlmacen(String rucCompanya, int codAlmacen) {
        Connection cn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;

        AimarAlmacen almacen = null;

        try {
            cn = Service.getConexion();
            String sql = "SELECT ruc_companyia, Cod_Ai_Almacen, Nom_Ai_Almacen, "
                    + "Dir_Ai_Almacen, Local_Defecto_Ai_Almacen, flg_AbrevActivoFijo, "
                    + "num_orden, ValIng01_Ai_Almacen, ValIng02_Ai_Almacen, "
                    + "ValIng03_Ai_Almacen, ValIng04_Ai_Almacen, ValIng05_Ai_Almacen, "
                    + "ValIng06_Ai_Almacen, ValIng07_Ai_Almacen, ValIng08_Ai_Almacen, "
                    + "ValIng09_Ai_Almacen, ValIng10_Ai_Almacen, ValIng11_Ai_Almacen, "
                    + "ValIng12_Ai_Almacen, ValSal01_Ai_Almacen, ValSal02_Ai_Almacen, "
                    + "ValSal03_Ai_Almacen, ValSal04_Ai_Almacen, ValSal05_Ai_Almacen, "
                    + "ValSal06_Ai_Almacen, ValSal07_Ai_Almacen, ValSal08_Ai_Almacen, "
                    + "ValSal09_Ai_Almacen, ValSal10_Ai_Almacen, ValSal11_Ai_Almacen, "
                    + "ValSal12_Ai_Almacen, UltInv_Ai_Almacen, UltSal_Ai_Almacen, "
                    + "Centro_Ai_Almacen, Fec_Cie01_Ai_Almacen, Fec_Cie02_Ai_Almacen, "
                    + "Fec_Cie03_Ai_Almacen, Fec_Cie04_Ai_Almacen, Fec_Cie05_Ai_Almacen, "
                    + "Fec_Cie06_Ai_Almacen, Fec_Cie07_Ai_Almacen, Fec_Cie08_Ai_Almacen, "
                    + "Fec_Cie09_Ai_Almacen, Fec_Cie10_Ai_Almacen, Fec_Cie11_Ai_Almacen, "
                    + "Fec_Cie12_Ai_Almacen, Cod_Tip_Prod, TipoCosto_Ai_Almacen, "
                    + "Cod_Ai_Estado, fec_creacion, hor_creacion, cod_usuario_creacion, "
                    + "fec_actualizacion, hor_actualizacion, cod_usuario_actualizacion, "
                    + "flg_sinregistro "
                    + "FROM AIMAR_Almacen "
                    + "WHERE ruc_companyia=? AND Cod_Ai_Almacen=?;";

            ps = cn.prepareStatement(sql);
            ps.setString(1, rucCompanya);
            ps.setInt(2, codAlmacen);
            rs = ps.executeQuery();

            almacen = new AimarAlmacen();
            if (rs.next()) {
                almacen.setRucCompanyia(rs.getString("ruc_companyia"));
                almacen.setCodAiAlmacen(rs.getInt("Cod_Ai_Almacen"));
                almacen.setNomAiAlmacen(rs.getString("Nom_Ai_Almacen"));
                almacen.setDirAiAlmacen(rs.getString("Dir_Ai_Almacen"));
                almacen.setLocalDefectoAiAlmacen(rs.getString("Local_Defecto_Ai_Almacen"));
                almacen.setFlgAbrevActivoFijo(rs.getString("flg_AbrevActivoFijo"));
                almacen.setNumOrden(rs.getInt("num_orden"));
                almacen.setValIng01AiAlmacen(rs.getString("ValIng01_Ai_Almacen"));
                almacen.setValIng02AiAlmacen(rs.getString("ValIng02_Ai_Almacen"));
                almacen.setValIng03AiAlmacen(rs.getString("ValIng03_Ai_Almacen"));
                almacen.setValIng04AiAlmacen(rs.getString("ValIng04_Ai_Almacen"));
                almacen.setValIng05AiAlmacen(rs.getString("ValIng05_Ai_Almacen"));
                almacen.setValIng06AiAlmacen(rs.getString("ValIng06_Ai_Almacen"));
                almacen.setValIng07AiAlmacen(rs.getString("ValIng07_Ai_Almacen"));
                almacen.setValIng08AiAlmacen(rs.getString("ValIng08_Ai_Almacen"));
                almacen.setValIng09AiAlmacen(rs.getString("ValIng09_Ai_Almacen"));
                almacen.setValIng10AiAlmacen(rs.getString("ValIng10_Ai_Almacen"));
                almacen.setValIng11AiAlmacen(rs.getString("ValIng11_Ai_Almacen"));
                almacen.setValIng12AiAlmacen(rs.getString("ValIng12_Ai_Almacen"));
                almacen.setValSal01AiAlmacen(rs.getString("ValSal01_Ai_Almacen"));
                almacen.setValSal02AiAlmacen(rs.getString("ValSal02_Ai_Almacen"));
                almacen.setValSal03AiAlmacen(rs.getString("ValSal03_Ai_Almacen"));
                almacen.setValSal04AiAlmacen(rs.getString("ValSal04_Ai_Almacen"));
                almacen.setValSal05AiAlmacen(rs.getString("ValSal05_Ai_Almacen"));
                almacen.setValSal06AiAlmacen(rs.getString("ValSal06_Ai_Almacen"));
                almacen.setValSal07AiAlmacen(rs.getString("ValSal07_Ai_Almacen"));
                almacen.setValSal08AiAlmacen(rs.getString("ValSal08_Ai_Almacen"));
                almacen.setValSal09AiAlmacen(rs.getString("ValSal09_Ai_Almacen"));
                almacen.setValSal10AiAlmacen(rs.getString("ValSal10_Ai_Almacen"));
                almacen.setValSal11AiAlmacen(rs.getString("ValSal11_Ai_Almacen"));
                almacen.setValSal12AiAlmacen(rs.getString("ValSal12_Ai_Almacen"));
                almacen.setUltInvAiAlmacen(rs.getDate("UltInv_Ai_Almacen"));
                almacen.setUltSalAiAlmacen(rs.getDate("UltSal_Ai_Almacen"));
                almacen.setCentroAiAlmacen(rs.getString("Centro_Ai_Almacen"));
                almacen.setFecCie01AiAlmacen(rs.getDate("Fec_Cie01_Ai_Almacen"));
                almacen.setFecCie02AiAlmacen(rs.getDate("Fec_Cie02_Ai_Almacen"));
                almacen.setFecCie03AiAlmacen(rs.getDate("Fec_Cie03_Ai_Almacen"));
                almacen.setFecCie04AiAlmacen(rs.getDate("Fec_Cie04_Ai_Almacen"));
                almacen.setFecCie05AiAlmacen(rs.getDate("Fec_Cie05_Ai_Almacen"));
                almacen.setFecCie06AiAlmacen(rs.getDate("Fec_Cie06_Ai_Almacen"));
                almacen.setFecCie07AiAlmacen(rs.getDate("Fec_Cie07_Ai_Almacen"));
                almacen.setFecCie08AiAlmacen(rs.getDate("Fec_Cie08_Ai_Almacen"));
                almacen.setFecCie09AiAlmacen(rs.getDate("Fec_Cie09_Ai_Almacen"));
                almacen.setFecCie10AiAlmacen(rs.getDate("Fec_Cie10_Ai_Almacen"));
                almacen.setFecCie11AiAlmacen(rs.getDate("Fec_Cie11_Ai_Almacen"));
                almacen.setFecCie12AiAlmacen(rs.getDate("Fec_Cie12_Ai_Almacen"));
                almacen.setCodTipProd(rs.getInt("Cod_Tip_Prod"));
                almacen.setTipoCostoAiAlmacen(rs.getString("TipoCosto_Ai_Almacen"));
                almacen.setCodAiEstado(rs.getString("Cod_Ai_Estado"));
                almacen.setFecCreacion(rs.getString("fec_creacion"));
                almacen.setHorCreacion(rs.getString("hor_creacion"));
                almacen.setCodUsuarioCreacion(rs.getInt("cod_usuario_creacion"));
                almacen.setFecActualizacion(rs.getString("fec_actualizacion"));
                almacen.setHorActualizacion(rs.getString("hor_actualizacion"));
                almacen.setCodUsuarioActualizacion(rs.getInt("cod_usuario_actualizacion"));
                almacen.setFlgSinregistro(rs.getString("flg_sinregistro"));
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
        return almacen;
    }

}
