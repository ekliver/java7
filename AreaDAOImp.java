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
import sys.dao.AreaDAO;
import sys.model.AgmaeArea;
import sys.model.AgmaeUsuario;
import sys.model.AvmovMovNotaDespachoCab;
import sys.util.Service;

/**
 *
 * @author Pc
 */
public class AreaDAOImp implements AreaDAO {

    @Override
    public List<AgmaeArea> listarAreas(AgmaeUsuario usuario) {
        Connection cn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        boolean existe = false;
        AgmaeArea area = null;
        List<AgmaeArea> lista = new ArrayList<>();
        try {
            cn = Service.getConexion();
            String sql = "SELECT AGMAE_Area.Cod_Area AS Cod_Area, "
                    + "AGMAE_Area.Des_Area AS Des_Area, "
                    + "AGMAE_Area.Des_Abre_Area AS Des_Abre_Area "
                    + "FROM AGMAE_Area INNER JOIN AIMAR_EstabXCCostoXArea "
                    + "ON (AGMAE_Area.ruc_companyia = AIMAR_EstabXCCostoXArea.ruc_companyia) "
                    + "AND (AGMAE_Area.Cod_Area = AIMAR_EstabXCCostoXArea.Cod_Area) "
                    + "where AIMAR_EstabXCCostoXArea.ruc_companyia=? "
                    + "AND AIMAR_EstabXCCostoXArea.cod_Establecimiento=? "
                    + "AND AIMAR_EstabXCCostoXArea.COD_CENTROC=?;";

            ps = cn.prepareStatement(sql);
            ps.setString(1, usuario.getRucCompanyia());
            ps.setString(2, usuario.getCodEstablecimiento());
            ps.setInt(3, usuario.getCodCentroc());
            rs = ps.executeQuery();
            while (rs.next()) {
                existe = true;
                area = new AgmaeArea();

                area.setCodArea(rs.getInt("Cod_Area"));
                area.setDesArea(rs.getString("Des_Area"));
                area.setDesAbreArea(rs.getString("Des_Abre_Area"));
                
                lista.add(area);
            }
            if (!existe) {
                area = null;
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
    public List<AgmaeArea> listarAreas(String rucCompanya) {
        Connection cn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        boolean existe = false;
        AgmaeArea area = null;
        List<AgmaeArea> lista = new ArrayList<>();
        try {
            cn = Service.getConexion();
            String sql = "SELECT ruc_companyia, Cod_Area, Des_Abre_Area, "
                    + "Des_Area, cod_centroc, flg_Defecto, flg_AbrevActivoFijo, "
                    + "Est_Area, fec_creacion, hor_creacion, cod_usuario_creacion, "
                    + "fec_actualizacion, hor_actualizacion, cod_usuario_actualizacion "
                    + "FROM AGMAE_Area "
                    + "WHERE ruc_companyia=?;";

            ps = cn.prepareStatement(sql);
            ps.setString(1, rucCompanya);

            rs = ps.executeQuery();
            while (rs.next()) {
                existe = true;
                area = new AgmaeArea();
                area.setRucCompanyia(rs.getString("ruc_companyia"));
                area.setCodArea(rs.getInt("Cod_Area"));
                area.setDesAbreArea(rs.getString("Des_Abre_Area"));
                area.setDesArea(rs.getString("Des_Area"));
                area.setCodCentroc(rs.getInt("cod_centroc"));
                area.setFlgDefecto(rs.getString("flg_Defecto"));
                area.setFlgAbrevActivoFijo(rs.getString("flg_AbrevActivoFijo"));
                area.setEstArea(rs.getString("Est_Area"));
                area.setFecCreacion(rs.getString("fec_creacion"));
                area.setHorCreacion(rs.getString("hor_creacion"));
                area.setCodUsuarioCreacion(rs.getInt("cod_usuario_creacion"));
                area.setFecActualizacion(rs.getString("fec_actualizacion"));
                area.setHorActualizacion(rs.getString("hor_actualizacion"));
                area.setCodUsuarioActualizacion(rs.getInt("cod_usuario_actualizacion"));

                lista.add(area);
            }
            if (!existe) {
                area = null;
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
    public AgmaeArea consultarObjArea(String rucCompanya, int codArea) {
        Connection cn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        AgmaeArea area = null;
        try {
            cn = Service.getConexion();
            String sql = "SELECT ruc_companyia, Cod_Area, Des_Abre_Area, "
                    + "Des_Area, cod_centroc, flg_Defecto, flg_AbrevActivoFijo, "
                    + "Est_Area, fec_creacion, hor_creacion, cod_usuario_creacion, "
                    + "fec_actualizacion, hor_actualizacion, cod_usuario_actualizacion "
                    + "FROM AGMAE_Area "
                    + "WHERE ruc_companyia=? AND Cod_Area=?;";

            ps = cn.prepareStatement(sql);
            ps.setString(1, rucCompanya);
            ps.setInt(2, codArea);
            rs = ps.executeQuery();

            area = new AgmaeArea();
            if (rs.next()) {
                area.setRucCompanyia(rs.getString("ruc_companyia"));
                area.setCodArea(rs.getInt("Cod_Area"));
                area.setDesAbreArea(rs.getString("Des_Abre_Area"));
                area.setDesArea(rs.getString("Des_Area"));
                area.setCodCentroc(rs.getInt("cod_centroc"));
                area.setFlgDefecto(rs.getString("flg_Defecto"));
                area.setFlgAbrevActivoFijo(rs.getString("flg_AbrevActivoFijo"));
                area.setEstArea(rs.getString("Est_Area"));
                area.setFecCreacion(rs.getString("fec_creacion"));
                area.setHorCreacion(rs.getString("hor_creacion"));
                area.setCodUsuarioCreacion(rs.getInt("cod_usuario_creacion"));
                area.setFecActualizacion(rs.getString("fec_actualizacion"));
                area.setHorActualizacion(rs.getString("hor_actualizacion"));
                area.setCodUsuarioActualizacion(rs.getInt("cod_usuario_actualizacion"));
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
        return area;

    }

}
