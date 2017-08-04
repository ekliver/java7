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
import sys.dao.CentroCostoDAO;
import sys.model.AgmaeCentrocosto;
import sys.model.AgmaeUsuario;
import sys.model.AvmovMovNotaDespachoCab;
import sys.util.Service;

/**
 *
 * @author Pc
 */
public class CentroCostoDAOImp implements CentroCostoDAO {

    @Override
    public List<AgmaeCentrocosto> listarCentros(AgmaeUsuario usuario) {
        Connection cn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        boolean existe = false;
        AgmaeCentrocosto cCosto = null;
        List<AgmaeCentrocosto> lista = new ArrayList<>();
        try {
            cn = Service.getConexion();
            String sql = "SELECT agmae_centrocosto.cod_centroc AS cod_centroc, "
                    + "agmae_centrocosto.des_centroc AS des_centroc "
                    + "FROM agmae_centrocosto INNER JOIN AIMAR_EstabXCCosto ON agmae_centrocosto.cod_centroc = AIMAR_EstabXCCosto.cod_centroc "
                    + "WHERE AIMAR_EstabXCCosto.ruc_companyia=? "
                    + "AND AIMAR_EstabXCCosto.COD_ESTABLECIMIENTO=?;";

            ps = cn.prepareStatement(sql);
            ps.setString(1, usuario.getRucCompanyia());
            ps.setString(2, usuario.getCodEstablecimiento());
            rs = ps.executeQuery();
            while (rs.next()) {
                existe = true;
                cCosto = new AgmaeCentrocosto();
                cCosto.setCodCentroc(rs.getInt("cod_centroc"));
                cCosto.setDesCentroc(rs.getString("des_centroc"));
                
                lista.add(cCosto);
            }
            if (!existe) {
                cCosto = null;
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
    public AgmaeCentrocosto consultarObjCentroCosto(int codCentroCosto) {
        Connection cn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        AgmaeCentrocosto cCosto = null;

        try {
            cn = Service.getConexion();
            String sql = "SELECT cod_centroc, des_centroc, ces_centroc, "
                    + "flg_AbrevActivoFijo, FEC_Creacion, HOR_Creacion, "
                    + "cod_usuario_creacion, fec_actualizacion, hor_actualizacion, "
                    + "cod_usuario_actualizacion "
                    + "FROM agmae_centrocosto "
                    + "WHERE cod_centroc=?;";

            ps = cn.prepareStatement(sql);
            ps.setInt(1, codCentroCosto);
            rs = ps.executeQuery();

            cCosto = new AgmaeCentrocosto();
            if (rs.next()) {
                cCosto.setCodCentroc(rs.getInt("cod_centroc"));
                cCosto.setDesCentroc(rs.getString("des_centroc"));
                cCosto.setCesCentroc(rs.getString("ces_centroc"));
                cCosto.setFlgAbrevActivoFijo(rs.getString("flg_AbrevActivoFijo"));
                cCosto.setFecCreacion(rs.getString("FEC_Creacion"));
                cCosto.setHorCreacion(rs.getString("HOR_Creacion"));
                cCosto.setCodUsuarioCreacion(rs.getInt("cod_usuario_creacion"));
                cCosto.setFecActualizacion(rs.getString("fec_actualizacion"));
                cCosto.setHorActualizacion(rs.getString("hor_actualizacion"));
                cCosto.setCodUsuarioActualizacion(rs.getInt("cod_usuario_actualizacion"));

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
        return cCosto;
    }

}
